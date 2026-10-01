"""
Shrinking the voter set of a KRaft quorum down to a single surviving controller, by
appending ``VotersRecord`` control batches to its local copy of the metadata log.
"""

import datetime
import itertools
import logging
import os
import shutil
import uuid

from collections.abc import Collection
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from kio.schema.types import BrokerId
from kio.schema.voters_record.v0.data import Voter
from kio.schema.voters_record.v0.data import VotersRecord
from kio.static.primitive import TZAwareMicros
from kio.static.primitive import i64

from .errors import CopiedVotersRecordNotFound
from .errors import DirectoryIdMismatch
from .errors import InsufficientPrivileges
from .errors import InvalidDirectories
from .errors import NoLogSegments
from .errors import VotersRecordNotFound
from .files import FileAttributes
from .files import can_apply
from .files import copy_new_file
from .files import fill_empty_file
from .files import fsync_directory
from .files import truncate_file
from .files import write_new_file
from .meta_properties import read_meta_properties
from .metadata_log import EffectiveLog
from .metadata_log import IncompleteBatch
from .metadata_log import LogEnd
from .metadata_log import Segment
from .metadata_log import Snapshot
from .metadata_log import SortedSegments
from .metadata_log import demotion_sequence
from .metadata_log import encode_voters_batch
from .metadata_log import find_voter
from .metadata_log import list_segments
from .metadata_log import list_snapshots
from .metadata_log import log_directory
from .metadata_log import read_log
from .metadata_log import resolve_log
from .metadata_log import segment_path
from .metadata_log import select_snapshot

LOG: Final = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True, kw_only=True)
class LocalReplica:
    node_id: BrokerId
    # The directory ID of the local replica, or None to skip verifying it against the
    # voter set.
    directory_id: uuid.UUID | None


def local_replica_from_meta_properties(metadata_dir: Path) -> LocalReplica:
    meta_properties = read_meta_properties(log_directory(metadata_dir))
    return LocalReplica(
        node_id=meta_properties.node_id,
        directory_id=meta_properties.directory_id,
    )


def _voter_ids(voters: VotersRecord) -> frozenset[BrokerId]:
    return frozenset(voter.voter_id for voter in voters.voters)


@dataclass(frozen=True, slots=True, kw_only=True)
class LogCopy:
    """
    Log segments and latest snapshot to replace those of the metadata directory with.
    """

    source_dir: Path
    segments: SortedSegments
    snapshot: Snapshot | None
    # The segments and snapshots of the metadata directory that the copy replaces.
    replaced: tuple[Path, ...]

    def resolve(self) -> EffectiveLog | None:
        return resolve_log(self.source_dir, self.segments, self.snapshot)


@dataclass(frozen=True, slots=True, kw_only=True)
class RecoveryPlan:
    metadata_dir: Path
    # When set, the log segments and snapshots of metadata_dir are replaced with this
    # copy before appending.
    log_copy: LogCopy | None
    log_end: LogEnd
    snapshot: Snapshot | None
    # Segments of the log that the snapshot is ahead of, which are deleted, or left out
    # of the copy, before appending.
    superseded_segments: tuple[Segment, ...]
    # The incomplete batch at the end of the log, which is truncated, or left out of the
    # copy, before appending.
    incomplete_batch: IncompleteBatch | None
    current_voters: VotersRecord
    kept_voter: Voter
    # The voter sets to append, in order, each one voter smaller than the previous.
    records: tuple[VotersRecord, ...]
    new_segment: Path
    # Whether new_segment is an existing, empty segment.
    new_segment_exists: bool
    # Kafka's leader epoch cache, deleted before segments are deleted or replaced.
    leader_epoch_checkpoint: Path | None
    file_attributes: FileAttributes

    @property
    def removed_voter_ids(self) -> tuple[BrokerId, ...]:
        return tuple(
            voter_id
            for before, after in itertools.pairwise(
                (self.current_voters, *self.records)
            )
            for voter_id in _voter_ids(before) - _voter_ids(after)
        )


def _read_log_copy(
    metadata_dir: Path,
    target_segments: SortedSegments,
    copy_from: Path,
) -> LogCopy:
    segments = list_segments(copy_from)
    if copy_from.resolve() == metadata_dir.resolve():
        raise InvalidDirectories(
            f"The directory to copy the log from, {copy_from}, is the metadata "
            f"directory itself."
        )
    snapshot = select_snapshot(copy_from, segments, list_snapshots(copy_from))
    return LogCopy(
        source_dir=copy_from,
        segments=segments,
        snapshot=snapshot,
        replaced=(
            *(segment.path for segment in target_segments),
            *(snapshot.path for snapshot in list_snapshots(metadata_dir)),
        ),
    )


def _target_file_attributes(
    metadata_dir: Path,
    target_segments: SortedSegments,
) -> FileAttributes:
    if target_segments:
        return FileAttributes.of(target_segments[-1].path)
    directory = FileAttributes.of(metadata_dir)
    return FileAttributes(mode=0o644, uid=directory.uid, gid=directory.gid)


def _leader_epoch_checkpoint(metadata_dir: Path) -> Path | None:
    # When recovering the log, Kafka only adds epochs newer than the latest one in the
    # checkpoint, so the checkpoint would keep the epochs of deleted segments. Kafka
    # creates a missing checkpoint, and then rebuilds it from the log.
    # https://github.com/apache/kafka/blob/26b251a451ce941d3d7a55e6487bcb7f16b5ad48/storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java#L503-L506
    # https://github.com/apache/kafka/blob/26b251a451ce941d3d7a55e6487bcb7f16b5ad48/server-common/src/main/java/org/apache/kafka/server/common/CheckpointFile.java#L64-L69
    path = metadata_dir / "leader-epoch-checkpoint"
    return path if path.exists() else None


def _verify_directory_id(voter: Voter, local_replica: LocalReplica) -> None:
    if local_replica.directory_id is None:
        return
    if voter.voter_directory_id != local_replica.directory_id:
        raise DirectoryIdMismatch(
            voter_id=voter.voter_id,
            local=local_replica.directory_id,
            registered=voter.voter_directory_id,
        )


def plan_recovery(
    *,
    metadata_dir: Path,
    local_replica: LocalReplica,
    copy_from: Path | None,
) -> RecoveryPlan:
    """
    Work out how to recover the controller with the metadata log in ``metadata_dir``,
    only reading from the file system.
    """
    target_segments = list_segments(metadata_dir)
    log_copy = (
        None
        if copy_from is None
        else _read_log_copy(metadata_dir, target_segments, copy_from)
    )

    file_attributes = _target_file_attributes(metadata_dir, target_segments)
    if not can_apply(file_attributes):
        raise InsufficientPrivileges(
            metadata_dir,
            file_attributes.uid,
            file_attributes.gid,
        )

    if log_copy is None:
        source_dir = metadata_dir
        log = read_log(metadata_dir)
    else:
        source_dir = log_copy.source_dir
        log = log_copy.resolve()

    if log is None:
        raise NoLogSegments(source_dir)
    if log.voters is None and log_copy is not None:
        raise CopiedVotersRecordNotFound(source_dir)
    if log.voters is None:
        raise VotersRecordNotFound(source_dir)

    kept_voter = find_voter(log.voters, local_replica.node_id)
    _verify_directory_id(kept_voter, local_replica)
    records = demotion_sequence(log.voters, local_replica.node_id)

    return RecoveryPlan(
        metadata_dir=metadata_dir,
        log_copy=log_copy,
        log_end=log.end,
        snapshot=log.snapshot,
        superseded_segments=log.superseded,
        incomplete_batch=log.incomplete_batch,
        current_voters=log.voters,
        kept_voter=kept_voter,
        records=records,
        # Name the segment after the offset of its first batch, like Kafka does when
        # rolling a segment. Kafka finds the segment to read from by the base offset
        # in its name, so any other name makes followers skip records.
        new_segment=segment_path(metadata_dir, log.next_offset),
        new_segment_exists=log.append_segment is not None,
        leader_epoch_checkpoint=(
            _leader_epoch_checkpoint(metadata_dir)
            if log_copy is not None or log.superseded
            else None
        ),
        file_attributes=file_attributes,
    )


def backup_destination(
    metadata_dir: Path,
    backup_dir: Path,
    now: datetime.datetime,
) -> Path:
    if backup_dir.resolve().is_relative_to(metadata_dir.resolve()):
        raise InvalidDirectories(
            f"The backup directory {backup_dir} must not be inside the metadata "
            f"directory {metadata_dir}."
        )
    name = Path(os.path.abspath(metadata_dir)).name
    timestamp = now.astimezone(datetime.UTC)
    destination = backup_dir / f"{name}.{timestamp:%Y%m%dT%H%M%SZ}"
    if destination.exists():
        raise InvalidDirectories(f"The backup destination {destination} exists.")
    return destination


def _back_up(metadata_dir: Path, backup_path: Path) -> None:
    LOG.info("Backing up %s to %s", metadata_dir, backup_path)
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    # The metadata log can hold credentials, such as SCRAM hashes. Since copytree only
    # copies modes, not owners, keep everyone but the owner out of the backup.
    backup_path.mkdir(mode=0o700)
    shutil.copytree(
        metadata_dir,
        backup_path / Path(os.path.abspath(metadata_dir)).name,
        symlinks=True,
    )


def _delete_files(paths: Iterable[Path]) -> None:
    for path in paths:
        LOG.info("Deleting %s", path)
        path.unlink()


def _truncate_incomplete_batch(path: Path, incomplete_batch: IncompleteBatch) -> None:
    """
    Truncate the incomplete batch from ``path``, which is either the segment holding
    it, or a copy of that segment.
    """
    LOG.info(
        "Truncating the incomplete batch at byte position %d in %s",
        incomplete_batch.position,
        path,
    )
    truncate_file(
        path,
        incomplete_batch.position,
        expected_size=incomplete_batch.segment_size,
    )


def _replace_log(
    metadata_dir: Path,
    log_copy: LogCopy,
    superseded: Collection[Segment],
    incomplete_batch: IncompleteBatch | None,
    attributes: FileAttributes,
) -> None:
    _delete_files(log_copy.replaced)
    for segment in log_copy.segments:
        if segment in superseded:
            continue
        destination = metadata_dir / segment.path.name
        LOG.info("Copying %s to %s", segment.path, destination)
        copy_new_file(segment.path, destination, attributes)
        if incomplete_batch is not None and incomplete_batch.segment == segment:
            _truncate_incomplete_batch(destination, incomplete_batch)
    if log_copy.snapshot is not None:
        source = log_copy.snapshot.path
        destination = metadata_dir / source.name
        LOG.info("Copying %s to %s", source, destination)
        copy_new_file(
            source,
            destination,
            # Kafka makes snapshots read-only once written.
            # https://github.com/apache/kafka/blob/26b251a451ce941d3d7a55e6487bcb7f16b5ad48/raft/src/main/java/org/apache/kafka/snapshot/FileRawSnapshotWriter.java#L115
            FileAttributes(mode=0o400, uid=attributes.uid, gid=attributes.gid),
        )


def execute_recovery(
    plan: RecoveryPlan,
    backup_path: Path,
    now: datetime.datetime,
) -> None:
    _back_up(plan.metadata_dir, backup_path)

    if plan.log_copy is None and plan.incomplete_batch is not None:
        _truncate_incomplete_batch(
            plan.incomplete_batch.segment.path,
            plan.incomplete_batch,
        )
    if plan.leader_epoch_checkpoint is not None:
        _delete_files((plan.leader_epoch_checkpoint,))
    if plan.log_copy is None:
        _delete_files(segment.path for segment in plan.superseded_segments)
    else:
        _replace_log(
            plan.metadata_dir,
            plan.log_copy,
            plan.superseded_segments,
            plan.incomplete_batch,
            plan.file_attributes,
        )
    fsync_directory(plan.metadata_dir)

    # Kafka record timestamps have millisecond precision, truncating to whole seconds
    # keeps the conversion exact.
    timestamp = TZAwareMicros.parse(now.astimezone(datetime.UTC).replace(microsecond=0))
    content = b"".join(
        encode_voters_batch(
            voters_record=record,
            offset=i64(plan.log_end.last_offset + 1 + index),
            partition_leader_epoch=plan.log_end.partition_leader_epoch,
            timestamp=timestamp,
        )
        for index, record in enumerate(plan.records)
    )
    LOG.info(
        "Writing %d VotersRecord batches to %s",
        len(plan.records),
        plan.new_segment,
    )
    if plan.new_segment_exists:
        fill_empty_file(plan.new_segment, content, plan.file_attributes)
    else:
        write_new_file(plan.new_segment, content, plan.file_attributes)
