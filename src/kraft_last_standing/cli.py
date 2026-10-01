import argparse
import datetime
import json
import logging
import os
import sys
import textwrap
import uuid

from collections.abc import Iterable
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final
from typing import assert_never

from kio.schema.types import BrokerId

from . import __version__
from .errors import KRaftLastStandingError
from .meta_properties import format_kafka_uuid
from .metadata_log import IncompleteBatch
from .metadata_log import Snapshot
from .probe import ProbeResult
from .probe import most_progressed_first
from .probe import probe
from .recovery import LocalReplica
from .recovery import RecoveryPlan
from .recovery import backup_destination
from .recovery import execute_recovery
from .recovery import local_replica_from_meta_properties
from .recovery import plan_recovery

LOG: Final = logging.getLogger(__name__)

RUNBOOK_URL: Final = "https://aiven-open.github.io/kraft-last-standing/latest/runbook/"


@dataclass(frozen=True, slots=True, kw_only=True)
class ProbeCommand:
    metadata_dirs: tuple[Path, ...]
    json: bool


@dataclass(frozen=True, slots=True, kw_only=True)
class RecoverCommand:
    metadata_dir: Path
    backup_dir: Path
    node_id: BrokerId | None
    copy_log_from: Path | None
    yes: bool


type Command = ProbeCommand | RecoverCommand


def _directory(value: str) -> Path:
    return Path(os.path.abspath(value))


def _node_id(value: str) -> BrokerId:
    try:
        return BrokerId.parse(int(value))
    except (ValueError, TypeError):
        raise argparse.ArgumentTypeError(f"invalid node ID: {value!r}") from None


def _probe_command(args: argparse.Namespace) -> ProbeCommand:
    return ProbeCommand(metadata_dirs=tuple(args.metadata_dirs), json=args.json)


def _recover_command(args: argparse.Namespace) -> RecoverCommand:
    return RecoverCommand(
        metadata_dir=args.metadata_dir,
        backup_dir=args.backup_dir,
        node_id=args.node_id,
        copy_log_from=args.copy_log_from,
        yes=args.yes,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kraft-last-standing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            "Recover an Apache Kafka KRaft cluster that has permanently lost "
            "controller quorum."
        ),
        epilog=f"Read the runbook before use: {RUNBOOK_URL}",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    subparsers = parser.add_subparsers(required=True, metavar="COMMAND")

    probe_parser = subparsers.add_parser(
        "probe",
        help="Show how far copies of the metadata log have progressed.",
        description=(
            "Show the last offset, leader epoch and voter set of local copies of the "
            "__cluster_metadata topic, most progressed first. Only reads."
        ),
    )
    probe_parser.set_defaults(parse_command=_probe_command)
    probe_parser.add_argument(
        "metadata_dirs",
        nargs="+",
        type=_directory,
        metavar="METADATA_DIR",
        help="A __cluster_metadata-0 directory.",
    )
    probe_parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as a JSON array.",
    )

    recover_parser = subparsers.add_parser(
        "recover",
        help="Shrink the voter set down to only the local controller.",
        description=(
            "Back up the metadata log of a stopped controller, then append records "
            "that remove every other voter from the voter set, one at a time."
        ),
    )
    recover_parser.set_defaults(parse_command=_recover_command)
    recover_parser.add_argument(
        "--metadata-dir",
        required=True,
        type=_directory,
        help="The __cluster_metadata-0 directory of the controller to recover.",
    )
    recover_parser.add_argument(
        "--backup-dir",
        required=True,
        type=_directory,
        help=(
            "Directory to back up the metadata directory into before modifying it. "
            "Created if missing. The backup is only accessible to its owner."
        ),
    )
    recover_parser.add_argument(
        "--node-id",
        type=_node_id,
        help=(
            "ID of the voter to keep. Defaults to node.id in the meta.properties of "
            "the log directory. When given, the directory ID of the local replica "
            "isn't verified."
        ),
    )
    recover_parser.add_argument(
        "--copy-log-from",
        type=_directory,
        metavar="METADATA_DIR",
        help=(
            "Replace the log segments and snapshots of the metadata directory with "
            "copies of the log segments and latest snapshot in this "
            "__cluster_metadata-0 directory, before appending. Use this when another "
            "node has a more progressed copy of the log."
        ),
    )
    recover_parser.add_argument(
        "--yes",
        action="store_true",
        help="Don't ask for confirmation.",
    )

    return parser


def parse_command(argv: Sequence[str] | None) -> Command:
    args = _build_parser().parse_args(argv)
    command: Command = args.parse_command(args)
    return command


def _format_rows(rows: Iterable[tuple[str, str]]) -> Iterable[str]:
    rows = tuple(rows)
    width = max(len(label) for label, _ in rows) + 1
    return (f"  {label + ':':<{width}} {value}" for label, value in rows)


def _format_ids(ids: Iterable[BrokerId]) -> str:
    return ", ".join(str(node_id) for node_id in ids)


def _format_snapshot(snapshot: Snapshot | None) -> str:
    return "none" if snapshot is None else snapshot.path.name


def _format_probe_result(result: ProbeResult) -> str:
    rows = (
        (
            "node ID",
            "unknown, found no meta.properties"
            if result.node_id is None
            else str(result.node_id),
        ),
        (
            "last offset",
            "none, found no record batches"
            if result.log_end is None
            else str(result.log_end.last_offset),
        ),
        (
            "leader epoch",
            "none"
            if result.log_end is None
            else str(result.log_end.partition_leader_epoch),
        ),
        ("snapshot", _format_snapshot(result.snapshot)),
        (
            "voters",
            "unknown, found no VotersRecord in the log or snapshot"
            if result.voter_ids is None
            else _format_ids(result.voter_ids),
        ),
    )
    return "\n".join((str(result.directory), *_format_rows(rows)))


def format_probe_text(results: Iterable[ProbeResult]) -> str:
    return "\n\n".join(_format_probe_result(result) for result in results)


def format_probe_json(results: Iterable[ProbeResult]) -> str:
    return json.dumps(
        [
            {
                "directory": str(result.directory),
                "node_id": result.node_id,
                "last_offset": (
                    None if result.log_end is None else result.log_end.last_offset
                ),
                "leader_epoch": (
                    None
                    if result.log_end is None
                    else result.log_end.partition_leader_epoch
                ),
                "snapshot": (
                    None if result.snapshot is None else result.snapshot.path.name
                ),
                "voter_ids": (
                    None if result.voter_ids is None else list(result.voter_ids)
                ),
            }
            for result in results
        ],
        indent=2,
    )


def _format_directory_id(directory_id: uuid.UUID | None) -> str:
    return "none" if directory_id is None else format_kafka_uuid(directory_id)


def _format_incomplete_batch(batch: IncompleteBatch, *, copying: bool) -> str:
    action = "left out of the copy" if copying else "truncated"
    return (
        f"at byte position {batch.position} of {batch.segment.path.name}, which is "
        f"{action}"
    )


def format_plan(plan: RecoveryPlan, backup_path: Path) -> str:
    first_offset = plan.log_end.last_offset + 1
    last_offset = plan.log_end.last_offset + len(plan.records)
    kept_voter = plan.kept_voter
    directory_id = _format_directory_id(kept_voter.voter_directory_id)
    copy_rows = (
        ()
        if plan.log_copy is None
        else (
            (
                "replace log with",
                f"a copy of the one in {plan.log_copy.source_dir}",
            ),
        )
    )
    superseded_rows = (
        (
            (
                "segments to delete" if plan.log_copy is None else "segments to skip",
                ", ".join(segment.path.name for segment in plan.superseded_segments)
                + ", which the snapshot is ahead of",
            ),
        )
        if plan.superseded_segments
        else ()
    )
    incomplete_rows = (
        ()
        if plan.incomplete_batch is None
        else (
            (
                "incomplete batch",
                _format_incomplete_batch(
                    plan.incomplete_batch,
                    copying=plan.log_copy is not None,
                ),
            ),
        )
    )
    checkpoint_rows = (
        ()
        if plan.leader_epoch_checkpoint is None
        else (
            (
                "file to delete",
                f"{plan.leader_epoch_checkpoint.name}, which Kafka rebuilds from the log",
            ),
        )
    )
    rows = (
        ("metadata directory", str(plan.metadata_dir)),
        *copy_rows,
        ("backup destination", str(backup_path)),
        ("last offset", str(plan.log_end.last_offset)),
        ("leader epoch", str(plan.log_end.partition_leader_epoch)),
        ("snapshot", _format_snapshot(plan.snapshot)),
        *superseded_rows,
        *incomplete_rows,
        *checkpoint_rows,
        ("current voters", _format_ids(v.voter_id for v in plan.current_voters.voters)),
        ("voter to keep", f"{kept_voter.voter_id} (directory ID {directory_id})"),
        ("voters to remove", f"{_format_ids(plan.removed_voter_ids)}, in this order"),
        (
            "new log segment",
            f"{plan.new_segment} (existing, empty)"
            if plan.new_segment_exists
            else str(plan.new_segment),
        ),
        ("offsets to append", f"{first_offset} to {last_offset}"),
    )
    return "\n".join(("Recovery plan", *_format_rows(rows)))


def _next_steps(plan: RecoveryPlan) -> str:
    return textwrap.dedent(
        f"""\
        Recovery complete. Next steps:

        1. Start the controller on this node, node {plan.kept_voter.voter_id}.
           Keep every other controller stopped.
        2. Verify that it is the only voter, and the leader:
           kafka-metadata-quorum.sh --bootstrap-controller HOST:PORT describe --status
        3. Add new controllers to restore fault tolerance, as described in the runbook:
           {RUNBOOK_URL}"""
    )


def _confirm() -> bool:
    try:
        answer = input("Modify the metadata log as described above? [y/N] ")
    except EOFError:
        return False
    return answer.strip().lower() in ("y", "yes")


def _run_probe(command: ProbeCommand) -> int:
    results = sorted(
        (probe(directory) for directory in command.metadata_dirs),
        key=most_progressed_first,
    )
    print(format_probe_json(results) if command.json else format_probe_text(results))
    return 0


def _local_replica(command: RecoverCommand) -> LocalReplica:
    if command.node_id is None:
        return local_replica_from_meta_properties(command.metadata_dir)
    return LocalReplica(node_id=command.node_id, directory_id=None)


def _run_recover(command: RecoverCommand, now: datetime.datetime) -> int:
    plan = plan_recovery(
        metadata_dir=command.metadata_dir,
        local_replica=_local_replica(command),
        copy_from=command.copy_log_from,
    )
    backup_path = backup_destination(command.metadata_dir, command.backup_dir, now)
    print(format_plan(plan, backup_path))
    if command.node_id is not None:
        LOG.warning(
            "The voter to keep was given with --node-id, so meta.properties was not "
            "read, and the directory ID of the local replica was not verified against "
            "the voter set."
        )

    if not command.yes and not _confirm():
        print("Aborted, nothing was modified.", file=sys.stderr)
        return 1

    execute_recovery(plan, backup_path, now)
    print()
    print(_next_steps(plan))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    command = parse_command(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
        stream=sys.stderr,
    )
    try:
        match command:
            case ProbeCommand():
                return _run_probe(command)
            case RecoverCommand():
                return _run_recover(command, datetime.datetime.now(datetime.UTC))
            case no_match:
                assert_never(no_match)
    except KRaftLastStandingError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
