"""
Inspecting copies of the metadata log, to find the one that has progressed furthest.
"""

from dataclasses import dataclass
from pathlib import Path

from kio.schema.types import BrokerId

from .errors import MetaPropertiesNotFound
from .meta_properties import read_meta_properties
from .metadata_log import LogEnd
from .metadata_log import Snapshot
from .metadata_log import log_directory
from .metadata_log import read_log


@dataclass(frozen=True, slots=True, kw_only=True)
class ProbeResult:
    directory: Path
    # None when the log directory has no meta.properties.
    node_id: BrokerId | None
    # None when there are no record batches in the log segments, and no snapshot
    # ahead of them.
    log_end: LogEnd | None
    # The snapshot Kafka loads on startup, if any.
    snapshot: Snapshot | None
    # None when neither the log nor the snapshot contains a VotersRecord.
    voter_ids: tuple[BrokerId, ...] | None


def _node_id(metadata_dir: Path) -> BrokerId | None:
    try:
        return read_meta_properties(log_directory(metadata_dir)).node_id
    except MetaPropertiesNotFound:
        return None


def probe(metadata_dir: Path) -> ProbeResult:
    log = read_log(metadata_dir)
    return ProbeResult(
        directory=metadata_dir,
        node_id=_node_id(metadata_dir),
        log_end=None if log is None else log.end,
        snapshot=None if log is None else log.snapshot,
        voter_ids=(
            None
            if log is None or log.voters is None
            else tuple(voter.voter_id for voter in log.voters.voters)
        ),
    )


def most_progressed_first(result: ProbeResult) -> tuple[bool, int, int]:
    """
    Sort key that orders results the way Raft compares logs, by leader epoch and then
    by last offset, highest first, followed by results without any record batches. A
    copy that diverged under an earlier leader can have a higher last offset, but its
    divergent tail was never committed.
    """
    if result.log_end is None:
        return True, 0, 0
    return (
        False,
        -result.log_end.partition_leader_epoch,
        -result.log_end.last_offset,
    )
