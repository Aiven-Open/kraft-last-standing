"""
Reading, and appending to, a local copy of the ``__cluster_metadata`` Raft log.
"""

import io
import logging
import os
import re

from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO
from typing import Final
from typing import NewType

from kio.records.readers import read_batch
from kio.records.schema import NewRecordBatch
from kio.records.schema import Record
from kio.records.schema import RecordBatch
from kio.records.writers import write_new_batch
from kio.schema.types import BrokerId
from kio.schema.voters_record.v0.data import Voter
from kio.schema.voters_record.v0.data import VotersRecord
from kio.serial import entity_reader
from kio.serial import entity_writer
from kio.serial.errors import DecodeError
from kio.serial.readers import read_int16
from kio.serial.readers import read_int32
from kio.serial.writers import write_int16
from kio.static.primitive import TZAwareMicros
from kio.static.primitive import i8
from kio.static.primitive import i16
from kio.static.primitive import i32
from kio.static.primitive import i64

from .errors import CorruptBatch
from .errors import InconsistentLog
from .errors import InvalidDirectories
from .errors import InvalidSegmentName
from .errors import InvalidSnapshotName
from .errors import NothingToRecover
from .errors import VoterNotFound

LOG: Final = logging.getLogger(__name__)

# https://github.com/apache/kafka/blob/e3430fef888b504f2e5ddf35637a4057430d198f/clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java#L134
CONTROL_FLAG_MASK: Final = 0b100000
# https://github.com/apache/kafka/blob/e3430fef888b504f2e5ddf35637a4057430d198f/clients/src/main/java/org/apache/kafka/common/record/ControlRecordType.java#L55-L62
CONTROL_RECORD_KEY_VERSION: Final = i16(0)
VOTERS_RECORD_TYPE: Final = i16(6)

# How Kafka frames record batches when reading a segment, which decides where it
# truncates an incomplete batch on startup.
# https://github.com/apache/kafka/blob/26b251a451ce941d3d7a55e6487bcb7f16b5ad48/clients/src/main/java/org/apache/kafka/common/record/internal/FileLogInputStream.java#L64-L82
_SIZE_POSITION: Final = 8
_LOG_OVERHEAD: Final = 12
_HEADER_SIZE_UP_TO_MAGIC: Final = 17
_MIN_BATCH_SIZE: Final = 14

_write_voters_record: Final = entity_writer(VotersRecord)
_read_voters_record: Final = entity_reader(VotersRecord)


@dataclass(frozen=True, slots=True, kw_only=True)
class Segment:
    base_offset: int
    path: Path


SortedSegments = NewType("SortedSegments", tuple[Segment, ...])


def log_directory(metadata_dir: Path) -> Path:
    """
    The log directory holding the metadata partition directory, which is where
    ``meta.properties`` lives. The path is normalized lexically, so that relative paths
    like ``.`` work.

    >>> log_directory(Path("/var/lib/kafka/data/__cluster_metadata-0/"))
    PosixPath('/var/lib/kafka/data')
    """
    return Path(os.path.abspath(metadata_dir)).parent


def segment_path(directory: Path, base_offset: int) -> Path:
    """
    >>> segment_path(Path("__cluster_metadata-0"), 1622)
    PosixPath('__cluster_metadata-0/00000000000000001622.log')
    """
    return directory / f"{base_offset:020}.log"


def _parse_segment(path: Path) -> Segment:
    if not (path.stem.isascii() and path.stem.isdigit()):
        raise InvalidSegmentName(path)
    return Segment(base_offset=int(path.stem), path=path)


def list_segments(directory: Path) -> SortedSegments:
    if not directory.is_dir():
        raise InvalidDirectories(f"{directory} is not a directory.")
    return SortedSegments(
        tuple(
            sorted(
                (_parse_segment(path) for path in directory.glob("*.log")),
                key=lambda segment: segment.base_offset,
            )
        )
    )


def _file_size(file: BinaryIO) -> int:
    return os.fstat(file.fileno()).st_size


def _parse_batches(path: Path, file: BinaryIO, end: int) -> Iterator[RecordBatch]:
    """
    Parse the record batches before byte position ``end``, reading one batch at a time.
    """
    file.seek(0)
    position = 0
    while position < end:
        header = file.read(_LOG_OVERHEAD)
        if len(header) < _LOG_OVERHEAD:
            raise CorruptBatch(path, position)
        size, _ = read_int32(header, _SIZE_POSITION)
        if size < _MIN_BATCH_SIZE or position + _LOG_OVERHEAD + size > end:
            raise CorruptBatch(path, position)
        try:
            batch, _ = read_batch(header + file.read(size), 0)
        except (ValueError, DecodeError) as exception:
            raise CorruptBatch(path, position) from exception
        position += _LOG_OVERHEAD + size
        yield batch


def _read_batches(path: Path) -> Iterator[RecordBatch]:
    with path.open("rb") as file:
        yield from _parse_batches(path, file, _file_size(file))


def read_batches(segments: SortedSegments) -> Iterator[RecordBatch]:
    for segment in segments:
        yield from _read_batches(segment.path)


@dataclass(frozen=True, slots=True, kw_only=True)
class IncompleteBatch:
    """
    An incomplete record batch at the end of a segment, which a process killed while
    appending leaves behind, and which Kafka truncates on startup.
    """

    segment: Segment
    # The byte position of the batch, which is the size the segment is truncated to.
    position: int
    segment_size: int


def _incomplete_batch_position(path: Path, file: BinaryIO, size: int) -> int | None:
    position = 0
    while position < size:
        remaining = size - position
        if remaining <= _HEADER_SIZE_UP_TO_MAGIC:
            return position
        file.seek(position + _SIZE_POSITION)
        batch_size, _ = read_int32(file.read(4), 0)
        if batch_size < _MIN_BATCH_SIZE:
            raise CorruptBatch(path, position)
        if _LOG_OVERHEAD + batch_size > remaining:
            return position
        position += _LOG_OVERHEAD + batch_size
    return None


@dataclass(frozen=True, slots=True, kw_only=True)
class LogEnd:
    last_offset: i64
    partition_leader_epoch: i32


@dataclass(frozen=True, slots=True, kw_only=True)
class Snapshot:
    # The offset following the last record the snapshot covers, which is where the log
    # continues from, and the leader epoch of that record.
    end_offset: i64
    epoch: i32
    path: Path

    @property
    def end(self) -> LogEnd:
        return LogEnd(
            last_offset=i64(self.end_offset - 1),
            partition_leader_epoch=self.epoch,
        )


def snapshot_path(directory: Path, end_offset: int, epoch: int) -> Path:
    """
    >>> snapshot_path(Path("__cluster_metadata-0"), 1622, 5)
    PosixPath('__cluster_metadata-0/00000000000000001622-0000000005.checkpoint')
    """
    return directory / f"{end_offset:020}-{epoch:010}.checkpoint"


def _parse_snapshot(path: Path) -> Snapshot:
    # Kafka parses snapshot names by position, so the widths are fixed.
    # https://github.com/apache/kafka/blob/26b251a451ce941d3d7a55e6487bcb7f16b5ad48/raft/src/main/java/org/apache/kafka/snapshot/Snapshots.java#L112-L115
    match = re.fullmatch(r"(\d{20})-(\d{10})", path.stem, re.ASCII)
    if match is None:
        raise InvalidSnapshotName(path)
    end_offset, epoch = match.groups()
    try:
        return Snapshot(
            end_offset=i64(int(end_offset)),
            epoch=i32(int(epoch)),
            path=path,
        )
    except TypeError:
        raise InvalidSnapshotName(path) from None


def list_snapshots(directory: Path) -> tuple[Snapshot, ...]:
    """
    Partial snapshots, and snapshots marked for deletion, have other suffixes, and are
    ignored like Kafka ignores them.
    """
    return tuple(_parse_snapshot(path) for path in directory.glob("*.checkpoint"))


@dataclass(frozen=True, slots=True, kw_only=True)
class LogSummary:
    end: LogEnd
    # The most recent VotersRecord in the log segments, at or after the offset given
    # to summarize().
    voters: VotersRecord | None


def _control_record_type(key: bytes) -> i16:
    version, size = read_int16(key, 0)
    record_type, _ = read_int16(key, size)
    if version != CONTROL_RECORD_KEY_VERSION:
        LOG.warning(
            "Unknown control record key version %r, parsing as version %r.",
            version,
            CONTROL_RECORD_KEY_VERSION,
        )
    return record_type


def _voters_records(batch: RecordBatch) -> Iterator[VotersRecord]:
    if not batch.attributes & CONTROL_FLAG_MASK:
        return
    for record in batch.records:
        if record.key is None or record.value is None:
            continue
        if len(record.key) != 4:
            LOG.warning(
                "Unexpected key length %r for control record at offset %r, expected 4.",
                len(record.key),
                record.offset,
            )
            continue
        if _control_record_type(record.key) != VOTERS_RECORD_TYPE:
            continue
        voters_record, _ = _read_voters_record(record.value, 0)
        yield voters_record


def _last_offset(batch: RecordBatch) -> i64:
    return i64(batch.base_offset + batch.last_offset_delta)


def summarize(
    batches: Iterable[RecordBatch],
    voters_from: int = 0,
) -> LogSummary | None:
    """
    Find the end of the batches, and the most recent VotersRecord in the batches that
    end at or after ``voters_from``.
    """
    last_batch: RecordBatch | None = None
    voters: VotersRecord | None = None
    for batch in batches:
        last_batch = batch
        if _last_offset(batch) < voters_from:
            continue
        for voters_record in _voters_records(batch):
            voters = voters_record
    if last_batch is None:
        return None
    return LogSummary(
        end=LogEnd(
            last_offset=_last_offset(last_batch),
            partition_leader_epoch=last_batch.partition_leader_epoch,
        ),
        voters=voters,
    )


def snapshot_voters(snapshot: Snapshot) -> VotersRecord | None:
    summary = summarize(_read_batches(snapshot.path))
    return None if summary is None else summary.voters


def select_snapshot(
    directory: Path,
    segments: SortedSegments,
    snapshots: Iterable[Snapshot],
) -> Snapshot | None:
    """
    Pick the snapshot Kafka loads on startup. Kafka deletes snapshots that end before
    the first segment, and requires a snapshot to cover the offsets before it.
    """
    # https://github.com/apache/kafka/blob/26b251a451ce941d3d7a55e6487bcb7f16b5ad48/raft/src/main/java/org/apache/kafka/raft/internals/KafkaRaftLog.java#L762-L786
    log_start = segments[0].base_offset if segments else 0
    retained = tuple(
        snapshot for snapshot in snapshots if snapshot.end_offset >= log_start
    )
    if not retained:
        if log_start > 0:
            raise InconsistentLog(
                directory,
                f"the log segments start at offset {log_start}, and there is no "
                f"snapshot of the offsets before it.",
            )
        return None
    latest = max(retained, key=lambda snapshot: (snapshot.epoch, snapshot.end_offset))
    furthest = max(retained, key=lambda snapshot: snapshot.end_offset)
    if furthest.end_offset > latest.end_offset:
        raise InconsistentLog(
            directory,
            f"snapshot {furthest.path.name} ends at a later offset than "
            f"{latest.path.name}, but has a lower leader epoch.",
        )
    return latest


@dataclass(frozen=True, slots=True, kw_only=True)
class EffectiveLog:
    """The metadata log as Kafka sees it once it has started up."""

    end: LogEnd
    # The VotersRecord Kafka considers current, or None for a static quorum.
    voters: VotersRecord | None
    snapshot: Snapshot | None
    # Segments that Kafka deletes on startup, because the snapshot is ahead of them.
    superseded: tuple[Segment, ...]
    # The existing, empty segment that Kafka appends the next batch to, if any.
    append_segment: Segment | None
    # The incomplete batch at the end of the log, which Kafka truncates on startup.
    incomplete_batch: IncompleteBatch | None

    @property
    def next_offset(self) -> i64:
        return i64(self.end.last_offset + 1)


@dataclass(frozen=True, slots=True, kw_only=True)
class _SegmentSummary:
    segment: Segment
    # None when the segment is empty, once the incomplete batch is truncated.
    summary: LogSummary | None
    incomplete_batch: IncompleteBatch | None


def _summarize_segment(segment: Segment, voters_from: int) -> _SegmentSummary:
    with segment.path.open("rb") as file:
        size = _file_size(file)
        position = _incomplete_batch_position(segment.path, file, size)
        batches = _parse_batches(
            segment.path,
            file,
            size if position is None else position,
        )
        return _SegmentSummary(
            segment=segment,
            summary=summarize(batches, voters_from),
            incomplete_batch=(
                None
                if position is None
                else IncompleteBatch(
                    segment=segment,
                    position=position,
                    segment_size=size,
                )
            ),
        )


def _is_ahead(
    snapshot: Snapshot,
    segments: Sequence[_SegmentSummary],
    end: LogEnd | None,
) -> bool:
    # https://github.com/apache/kafka/blob/26b251a451ce941d3d7a55e6487bcb7f16b5ad48/raft/src/main/java/org/apache/kafka/raft/internals/KafkaRaftLog.java#L251
    latest_epoch = 0 if end is None else end.partition_leader_epoch
    end_offset = max(
        0 if end is None else end.last_offset + 1,
        segments[-1].segment.base_offset if segments else 0,
    )
    return snapshot.epoch > latest_epoch or (
        snapshot.epoch == latest_epoch and snapshot.end_offset > end_offset
    )


def _truncated_to(
    snapshot: Snapshot,
    segments: Sequence[_SegmentSummary],
) -> EffectiveLog:
    """
    The log after Kafka truncates it fully, starting it over at the end of the
    snapshot. An empty segment at that offset is what Kafka would recreate, so it's
    kept.
    """
    end_offset = snapshot.end_offset
    append = next(
        (
            summary
            for summary in segments
            if summary.summary is None and summary.segment.base_offset == end_offset
        ),
        None,
    )
    return EffectiveLog(
        end=snapshot.end,
        voters=snapshot_voters(snapshot),
        snapshot=snapshot,
        superseded=tuple(
            summary.segment for summary in segments if summary is not append
        ),
        append_segment=None if append is None else append.segment,
        incomplete_batch=None if append is None else append.incomplete_batch,
    )


def resolve_log(
    directory: Path,
    segments: SortedSegments,
    snapshot: Snapshot | None,
) -> EffectiveLog | None:
    """
    Combine the sorted segments and the snapshot selected by select_snapshot() the way
    Kafka does on startup. Returns None when there is neither a record batch in the
    segments, nor a snapshot ahead of them.
    """
    # Kafka reads control records from the snapshot, and then from the log, starting at
    # the end of the snapshot.
    # https://github.com/apache/kafka/blob/26b251a451ce941d3d7a55e6487bcb7f16b5ad48/raft/src/main/java/org/apache/kafka/raft/internals/KRaftControlRecordStateMachine.java#L259-L293
    voters_from = 0 if snapshot is None else snapshot.end_offset
    summaries = tuple(_summarize_segment(segment, voters_from) for segment in segments)
    # Kafka truncates the log at the first incomplete batch, and deletes the segments
    # after it, so only the last segment can end with one without losing records.
    # https://github.com/apache/kafka/blob/26b251a451ce941d3d7a55e6487bcb7f16b5ad48/storage/src/main/java/org/apache/kafka/storage/internals/log/LogLoader.java#L478-L483
    for summary in summaries[:-1]:
        if summary.incomplete_batch is not None:
            raise CorruptBatch(summary.segment.path, summary.incomplete_batch.position)
    non_empty = tuple(
        summary.summary for summary in summaries if summary.summary is not None
    )
    end = non_empty[-1].end if non_empty else None

    if snapshot is not None and _is_ahead(snapshot, summaries, end):
        return _truncated_to(snapshot, summaries)
    if end is None:
        return None

    next_offset = end.last_offset + 1
    for summary in summaries:
        if summary.segment.base_offset > next_offset:
            raise InconsistentLog(
                directory,
                f"segment {summary.segment.path.name} starts after the last record "
                f"batch, which ends at offset {end.last_offset}.",
            )
    if snapshot is not None and snapshot.end_offset > next_offset:
        raise InconsistentLog(
            directory,
            f"snapshot {snapshot.path.name} ends after the last record "
            f"batch, at offset {end.last_offset}, but has a lower leader epoch.",
        )

    log_voters = tuple(
        summary.voters for summary in non_empty if summary.voters is not None
    )
    voters: VotersRecord | None
    if log_voters:
        voters = log_voters[-1]
    elif snapshot is not None:
        voters = snapshot_voters(snapshot)
    else:
        voters = None
    return EffectiveLog(
        end=end,
        voters=voters,
        snapshot=snapshot,
        superseded=(),
        append_segment=next(
            (
                summary.segment
                for summary in summaries
                if summary.segment.base_offset == next_offset
            ),
            None,
        ),
        incomplete_batch=summaries[-1].incomplete_batch,
    )


def read_log(directory: Path) -> EffectiveLog | None:
    segments = list_segments(directory)
    snapshot = select_snapshot(directory, segments, list_snapshots(directory))
    return resolve_log(directory, segments, snapshot)


def find_voter(voters: VotersRecord, voter_id: BrokerId) -> Voter:
    for voter in voters.voters:
        if voter.voter_id == voter_id:
            return voter
    raise VoterNotFound(voter_id, (voter.voter_id for voter in voters.voters))


def demotion_sequence(
    voters: VotersRecord,
    keep: BrokerId,
) -> tuple[VotersRecord, ...]:
    """
    Build the sequence of voter sets that shrinks the quorum down to only the voter
    with ID ``keep``, removing a single voter at a time.
    """
    kept = find_voter(voters, keep)
    others = tuple(voter for voter in voters.voters if voter.voter_id != keep)
    if not others:
        raise NothingToRecover(keep)
    return tuple(
        VotersRecord(version=i16(0), voters=(kept, *others[:remaining]))
        for remaining in reversed(range(len(others)))
    )


def _control_record_key(record_type: i16) -> bytes:
    with io.BytesIO() as buffer:
        write_int16(buffer, CONTROL_RECORD_KEY_VERSION)
        write_int16(buffer, record_type)
        return buffer.getvalue()


def encode_voters_batch(
    voters_record: VotersRecord,
    offset: i64,
    partition_leader_epoch: i32,
    timestamp: TZAwareMicros,
) -> bytes:
    with io.BytesIO() as value_buffer:
        _write_voters_record(value_buffer, voters_record)
        value = value_buffer.getvalue()

    batch = NewRecordBatch(
        producer_id=i64(-1),
        producer_epoch=i16(-1),
        partition_leader_epoch=partition_leader_epoch,
        base_sequence=i32(-1),
        attributes=i16(CONTROL_FLAG_MASK),
        records=(
            Record(
                attributes=i8(0),
                timestamp=timestamp,
                offset=offset,
                key=_control_record_key(VOTERS_RECORD_TYPE),
                value=value,
                headers=(),
            ),
        ),
    )

    with io.BytesIO() as batch_buffer:
        write_new_batch(batch_buffer, batch)
        return batch_buffer.getvalue()
