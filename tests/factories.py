import datetime
import io
import uuid

from pathlib import Path
from typing import Final

from kio.records.schema import NewRecordBatch
from kio.records.schema import Record
from kio.records.writers import write_new_batch
from kio.schema.types import BrokerId
from kio.schema.voters_record.v0.data import Endpoint
from kio.schema.voters_record.v0.data import KRaftVersionFeature
from kio.schema.voters_record.v0.data import Voter
from kio.schema.voters_record.v0.data import VotersRecord
from kio.static.primitive import TZAwareMicros
from kio.static.primitive import i8
from kio.static.primitive import i16
from kio.static.primitive import i32
from kio.static.primitive import i64
from kio.static.primitive import u16

from kraft_last_standing.meta_properties import format_kafka_uuid
from kraft_last_standing.metadata_log import CONTROL_FLAG_MASK
from kraft_last_standing.metadata_log import encode_voters_batch
from kraft_last_standing.metadata_log import segment_path
from kraft_last_standing.metadata_log import snapshot_path

TIMESTAMP: Final = TZAwareMicros.parse(
    datetime.datetime(2026, 10, 1, 12, 0, tzinfo=datetime.UTC)
)
CLUSTER_ID: Final = "MkU3OEVBNTcwNTJENDM2Qg"


def directory_id(voter_id: int) -> uuid.UUID:
    return uuid.UUID(int=0x1000 + voter_id)


def voter(voter_id: int) -> Voter:
    return Voter(
        voter_id=BrokerId(voter_id),
        voter_directory_id=directory_id(voter_id),
        endpoints=(
            Endpoint(
                name="CONTROLLER",
                host=f"controller-{voter_id}",
                port=u16(9093),
            ),
        ),
        k_raft_version_feature=KRaftVersionFeature(
            min_supported_version=i16(0),
            max_supported_version=i16(1),
        ),
    )


def voters_record(*voter_ids: int) -> VotersRecord:
    return VotersRecord(
        version=i16(0),
        voters=tuple(voter(voter_id) for voter_id in voter_ids),
    )


def voters_batch(
    record: VotersRecord,
    offset: int,
    epoch: int = 1,
) -> bytes:
    return encode_voters_batch(
        voters_record=record,
        offset=i64(offset),
        partition_leader_epoch=i32(epoch),
        timestamp=TIMESTAMP,
    )


def _encode_batch(
    records: tuple[Record, ...],
    attributes: int,
    epoch: int,
) -> bytes:
    batch = NewRecordBatch(
        producer_id=i64(-1),
        producer_epoch=i16(-1),
        partition_leader_epoch=i32(epoch),
        base_sequence=i32(-1),
        attributes=i16(attributes),
        records=records,
    )
    with io.BytesIO() as buffer:
        write_new_batch(buffer, batch)
        return buffer.getvalue()


def data_batch(
    offset: int,
    record_count: int = 1,
    epoch: int = 1,
) -> bytes:
    return _encode_batch(
        records=tuple(
            Record(
                attributes=i8(0),
                timestamp=TIMESTAMP,
                offset=i64(offset + delta),
                key=None,
                value=b"metadata",
                headers=(),
            )
            for delta in range(record_count)
        ),
        attributes=0,
        epoch=epoch,
    )


def corrupt_batch(offset: int) -> bytes:
    """A complete data batch whose checksum doesn't match its content."""
    batch = data_batch(offset=offset)
    return batch[:-1] + bytes((batch[-1] ^ 0xFF,))


def control_batch(
    offset: int,
    key: bytes | None,
    value: bytes | None,
    epoch: int = 1,
) -> bytes:
    return _encode_batch(
        records=(
            Record(
                attributes=i8(0),
                timestamp=TIMESTAMP,
                offset=i64(offset),
                key=key,
                value=value,
                headers=(),
            ),
        ),
        attributes=CONTROL_FLAG_MASK,
        epoch=epoch,
    )


def write_segment(directory: Path, base_offset: int, *batches: bytes) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = segment_path(directory, base_offset)
    path.write_bytes(b"".join(batches))
    return path


def write_snapshot(
    directory: Path,
    end_offset: int,
    epoch: int,
    voters: VotersRecord | None,
) -> Path:
    """
    Write a snapshot that, like the ones Kafka writes, has offsets starting from zero
    and is read-only. It holds the voter set, when given, followed by a data batch.
    """
    directory.mkdir(parents=True, exist_ok=True)
    path = snapshot_path(directory, end_offset, epoch)
    path.write_bytes(
        (b"" if voters is None else voters_batch(voters, offset=0, epoch=epoch))
        + data_batch(offset=1, record_count=2, epoch=epoch)
    )
    path.chmod(0o400)
    return path


def write_meta_properties(
    log_directory: Path,
    node_id: int,
    directory: uuid.UUID,
) -> None:
    log_directory.mkdir(parents=True, exist_ok=True)
    (log_directory / "meta.properties").write_text(
        "#\n"
        "#Thu Oct 01 12:00:00 UTC 2026\n"
        f"cluster.id={CLUSTER_ID}\n"
        f"directory.id={format_kafka_uuid(directory)}\n"
        f"node.id={node_id}\n"
        "version=1\n"
    )


def write_controller_log(metadata_dir: Path) -> None:
    """
    Write a log with two segments, of a quorum with voters 0, 1 and 2, whose last
    offset is 9 at leader epoch 3.
    """
    write_segment(
        metadata_dir,
        0,
        voters_batch(voters_record(0, 1, 2), offset=0),
        data_batch(offset=1, record_count=3),
    )
    write_segment(
        metadata_dir,
        4,
        data_batch(offset=4, record_count=2, epoch=2),
        data_batch(offset=6, record_count=4, epoch=3),
    )
