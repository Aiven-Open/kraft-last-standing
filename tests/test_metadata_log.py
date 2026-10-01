import logging

from pathlib import Path

import pytest

from kio.records.readers import read_batch
from kio.schema.types import BrokerId
from kio.schema.voters_record.v0.data import VotersRecord
from kio.serial import entity_reader
from kio.static.primitive import i32
from kio.static.primitive import i64

from kraft_last_standing.errors import CorruptBatch
from kraft_last_standing.errors import InconsistentLog
from kraft_last_standing.errors import InvalidDirectories
from kraft_last_standing.errors import InvalidSegmentName
from kraft_last_standing.errors import InvalidSnapshotName
from kraft_last_standing.errors import NothingToRecover
from kraft_last_standing.errors import VoterNotFound
from kraft_last_standing.metadata_log import CONTROL_FLAG_MASK
from kraft_last_standing.metadata_log import EffectiveLog
from kraft_last_standing.metadata_log import IncompleteBatch
from kraft_last_standing.metadata_log import LogEnd
from kraft_last_standing.metadata_log import Segment
from kraft_last_standing.metadata_log import Snapshot
from kraft_last_standing.metadata_log import SortedSegments
from kraft_last_standing.metadata_log import demotion_sequence
from kraft_last_standing.metadata_log import find_voter
from kraft_last_standing.metadata_log import list_segments
from kraft_last_standing.metadata_log import list_snapshots
from kraft_last_standing.metadata_log import read_batches
from kraft_last_standing.metadata_log import read_log
from kraft_last_standing.metadata_log import segment_path
from kraft_last_standing.metadata_log import select_snapshot
from kraft_last_standing.metadata_log import snapshot_path
from kraft_last_standing.metadata_log import summarize

from .factories import TIMESTAMP
from .factories import control_batch
from .factories import corrupt_batch
from .factories import data_batch
from .factories import voter
from .factories import voters_batch
from .factories import voters_record
from .factories import write_controller_log
from .factories import write_segment
from .factories import write_snapshot


class TestListSegments:
    def test_returns_log_segments_sorted_by_base_offset(self, tmp_path: Path) -> None:
        for base_offset in (20, 3, 100):
            write_segment(tmp_path, base_offset)
        (tmp_path / "00000000000000000003.index").touch()
        (tmp_path / "00000000000000000100-0000000002.checkpoint").touch()
        (tmp_path / "quorum-state").touch()

        assert list_segments(tmp_path) == (
            Segment(base_offset=3, path=segment_path(tmp_path, 3)),
            Segment(base_offset=20, path=segment_path(tmp_path, 20)),
            Segment(base_offset=100, path=segment_path(tmp_path, 100)),
        )

    def test_returns_empty_tuple_for_directory_without_segments(
        self,
        tmp_path: Path,
    ) -> None:
        assert list_segments(tmp_path) == ()

    @pytest.mark.parametrize("name", ["foo.log", "-1.log", "١٢٣.log"])
    def test_raises_invalid_segment_name_for_non_numeric_name(
        self,
        tmp_path: Path,
        name: str,
    ) -> None:
        (tmp_path / name).touch()
        with pytest.raises(InvalidSegmentName):
            list_segments(tmp_path)

    def test_raises_invalid_directories_for_missing_directory(
        self,
        tmp_path: Path,
    ) -> None:
        with pytest.raises(InvalidDirectories, match="is not a directory"):
            list_segments(tmp_path / "missing")


class TestReadBatches:
    def test_reads_batches_across_segments_in_order(self, tmp_path: Path) -> None:
        write_controller_log(tmp_path)
        batches = tuple(read_batches(list_segments(tmp_path)))
        assert [batch.base_offset for batch in batches] == [0, 1, 4, 6]

    @pytest.mark.parametrize(
        "length",
        [
            # Truncated inside the batch header.
            10,
            # Truncated inside the records, failing checksum validation.
            40,
        ],
    )
    def test_raises_corrupt_batch_for_truncated_tail(
        self,
        tmp_path: Path,
        length: int,
    ) -> None:
        complete = data_batch(offset=0)
        tail = data_batch(offset=1)
        path = write_segment(tmp_path, 0, complete, tail[:length])

        with pytest.raises(CorruptBatch) as exc_info:
            tuple(read_batches(list_segments(tmp_path)))

        assert f"byte position {len(complete)} in {path}" in str(exc_info.value)

    def test_raises_corrupt_batch_for_batch_size_below_minimum(
        self,
        tmp_path: Path,
    ) -> None:
        path = write_segment(tmp_path, 0, bytes(18))
        with pytest.raises(CorruptBatch, match=f"byte position 0 in {path}"):
            tuple(read_batches(list_segments(tmp_path)))


class TestSummarize:
    def test_returns_none_for_no_batches(self) -> None:
        assert summarize(()) is None

    def test_returns_end_of_last_batch_and_most_recent_voters_record(
        self,
        tmp_path: Path,
    ) -> None:
        write_segment(
            tmp_path,
            0,
            voters_batch(voters_record(0, 1, 2), offset=0),
            data_batch(offset=1),
            voters_batch(voters_record(0, 1, 2, 3), offset=2, epoch=2),
            data_batch(offset=3, record_count=5, epoch=4),
        )

        summary = summarize(read_batches(list_segments(tmp_path)))

        assert summary is not None
        assert summary.end == LogEnd(last_offset=i64(7), partition_leader_epoch=i32(4))
        assert summary.voters == voters_record(0, 1, 2, 3)

    def test_returns_no_voters_when_log_has_no_voters_record(self) -> None:
        batch, _ = read_batch(data_batch(offset=5, record_count=2), 0)

        summary = summarize((batch,))

        assert summary is not None
        assert summary.end == LogEnd(last_offset=i64(6), partition_leader_epoch=i32(1))
        assert summary.voters is None

    def test_ignores_other_control_records(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        write_segment(
            tmp_path,
            0,
            voters_batch(voters_record(0, 1, 2), offset=0),
            # A LeaderChangeMessage, control record type 2.
            control_batch(offset=1, key=b"\x00\x00\x00\x02", value=b"ignored"),
            control_batch(offset=2, key=None, value=b"ignored"),
            control_batch(offset=3, key=b"\x00\x00\x00\x06", value=None),
            control_batch(offset=4, key=b"\x00\x00\x06", value=b"ignored"),
        )

        with caplog.at_level(logging.WARNING):
            summary = summarize(read_batches(list_segments(tmp_path)))

        assert summary is not None
        assert summary.voters == voters_record(0, 1, 2)
        assert summary.end.last_offset == 4
        assert "Unexpected key length 3 for control record at offset 4" in caplog.text

    def test_warns_about_unknown_control_record_key_version(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        valid = voters_batch(voters_record(0, 1), offset=0)
        record, _ = read_batch(valid, 0)
        [voters] = record.records
        assert voters.value is not None
        write_segment(
            tmp_path,
            0,
            control_batch(offset=0, key=b"\x00\x01\x00\x06", value=voters.value),
        )

        with caplog.at_level(logging.WARNING):
            summary = summarize(read_batches(list_segments(tmp_path)))

        assert summary is not None
        assert summary.voters == voters_record(0, 1)
        assert "Unknown control record key version 1" in caplog.text

    def test_ignores_voters_records_in_batches_ending_before_voters_from(
        self,
        tmp_path: Path,
    ) -> None:
        write_segment(
            tmp_path,
            0,
            voters_batch(voters_record(0, 1, 2), offset=0),
            voters_batch(voters_record(0, 1), offset=1),
            data_batch(offset=2, record_count=3),
        )

        summary = summarize(read_batches(list_segments(tmp_path)), voters_from=2)

        assert summary is not None
        assert summary.end.last_offset == 4
        assert summary.voters is None


class TestListSnapshots:
    def test_parses_end_offset_and_epoch_ignoring_other_files(
        self,
        tmp_path: Path,
    ) -> None:
        write_segment(tmp_path, 0)
        first = write_snapshot(tmp_path, 0, 0, None)
        second = write_snapshot(tmp_path, 1622, 5, None)
        (tmp_path / "00000000000000001700-0000000005.checkpoint.part").touch()
        (tmp_path / "00000000000000001500-0000000005.checkpoint.deleted").touch()
        (tmp_path / "leader-epoch-checkpoint").touch()

        assert sorted(list_snapshots(tmp_path), key=lambda s: s.end_offset) == [
            Snapshot(end_offset=i64(0), epoch=i32(0), path=first),
            Snapshot(end_offset=i64(1622), epoch=i32(5), path=second),
        ]

    @pytest.mark.parametrize(
        "name",
        [
            "foo.checkpoint",
            "1622-5.checkpoint",
            "00000000000000001622-00000000005.checkpoint",
            "00000000000000001622_0000000005.checkpoint",
            "١٢٣٤٥٦٧٨٩٠١٢٣٤٥٦٧٨٩٠-0000000005.checkpoint",
            # Out of range for the offset, and for the epoch.
            "99999999999999999999-0000000005.checkpoint",
            "00000000000000001622-9999999999.checkpoint",
        ],
    )
    def test_raises_invalid_snapshot_name(self, tmp_path: Path, name: str) -> None:
        (tmp_path / name).touch()
        with pytest.raises(InvalidSnapshotName):
            list_snapshots(tmp_path)


def _snapshot(end_offset: int, epoch: int) -> Snapshot:
    return Snapshot(
        end_offset=i64(end_offset),
        epoch=i32(epoch),
        path=snapshot_path(Path("__cluster_metadata-0"), end_offset, epoch),
    )


def _segments(*base_offsets: int) -> SortedSegments:
    return SortedSegments(
        tuple(
            Segment(base_offset=base_offset, path=segment_path(Path(), base_offset))
            for base_offset in base_offsets
        )
    )


class TestSelectSnapshot:
    def test_returns_none_without_snapshots(self) -> None:
        assert select_snapshot(Path(), _segments(0, 10), ()) is None

    def test_returns_latest_by_epoch_then_end_offset(self) -> None:
        snapshots = (
            _snapshot(0, 0),
            _snapshot(10, 2),
            _snapshot(20, 2),
            _snapshot(20, 3),
        )
        assert select_snapshot(Path(), _segments(0), snapshots) == _snapshot(20, 3)

    def test_ignores_snapshots_ending_before_first_segment(self) -> None:
        snapshots = (_snapshot(5, 4), _snapshot(10, 2), _snapshot(20, 3))
        assert select_snapshot(Path(), _segments(10), snapshots) == _snapshot(20, 3)

    def test_returns_snapshot_without_segments(self) -> None:
        assert select_snapshot(Path(), _segments(), (_snapshot(20, 3),)) == (
            _snapshot(20, 3)
        )

    def test_raises_inconsistent_log_for_missing_snapshot_of_log_start(self) -> None:
        with pytest.raises(
            InconsistentLog, match="start at offset 10, and there is no"
        ):
            select_snapshot(Path(), _segments(10, 20), (_snapshot(5, 2),))

    def test_raises_inconsistent_log_for_further_snapshot_with_lower_epoch(
        self,
    ) -> None:
        snapshots = (_snapshot(20, 3), _snapshot(30, 2))
        with pytest.raises(
            InconsistentLog,
            match=(
                r"00000000000000000030-0000000002.checkpoint ends at a later offset "
                r"than 00000000000000000020-0000000003.checkpoint"
            ),
        ):
            select_snapshot(Path(), _segments(0), snapshots)


class TestReadLog:
    def test_reads_log_without_snapshots(self, tmp_path: Path) -> None:
        write_controller_log(tmp_path)

        assert read_log(tmp_path) == EffectiveLog(
            end=LogEnd(last_offset=i64(9), partition_leader_epoch=i32(3)),
            voters=voters_record(0, 1, 2),
            snapshot=None,
            superseded=(),
            append_segment=None,
            incomplete_batch=None,
        )

    def test_prefers_voters_in_log_over_bootstrap_snapshot(
        self,
        tmp_path: Path,
    ) -> None:
        write_controller_log(tmp_path)
        bootstrap = write_snapshot(tmp_path, 0, 0, voters_record(0))

        log = read_log(tmp_path)

        assert log is not None
        assert log.end == LogEnd(last_offset=i64(9), partition_leader_epoch=i32(3))
        assert log.voters == voters_record(0, 1, 2)
        assert log.snapshot == Snapshot(end_offset=i64(0), epoch=i32(0), path=bootstrap)

    @pytest.mark.parametrize(
        ("voters_offset", "expected"),
        [
            pytest.param(14, voters_record(0, 1, 2), id="before-snapshot-end"),
            pytest.param(15, voters_record(0, 1), id="at-snapshot-end"),
        ],
    )
    def test_reads_voters_in_log_from_end_of_snapshot(
        self,
        tmp_path: Path,
        voters_offset: int,
        expected: VotersRecord,
    ) -> None:
        write_segment(
            tmp_path,
            10,
            data_batch(offset=10, record_count=voters_offset - 10, epoch=2),
            voters_batch(voters_record(0, 1), offset=voters_offset, epoch=2),
            data_batch(offset=voters_offset + 1, record_count=5, epoch=2),
        )
        write_snapshot(tmp_path, 15, 2, voters_record(0, 1, 2))

        log = read_log(tmp_path)

        assert log is not None
        assert log.end == LogEnd(
            last_offset=i64(voters_offset + 5),
            partition_leader_epoch=i32(2),
        )
        assert log.voters == expected
        assert log.superseded == ()

    def test_reads_voters_from_snapshot_at_end_of_log(self, tmp_path: Path) -> None:
        write_segment(tmp_path, 10, data_batch(offset=10, record_count=5, epoch=2))
        empty = write_segment(tmp_path, 15)
        write_snapshot(tmp_path, 15, 2, voters_record(0, 1, 2))

        log = read_log(tmp_path)

        assert log is not None
        assert log.end == LogEnd(last_offset=i64(14), partition_leader_epoch=i32(2))
        assert log.voters == voters_record(0, 1, 2)
        assert log.superseded == ()
        assert log.append_segment == Segment(base_offset=15, path=empty)

    def test_returns_empty_segment_after_last_batch(self, tmp_path: Path) -> None:
        write_controller_log(tmp_path)
        empty = write_segment(tmp_path, 10)

        log = read_log(tmp_path)

        assert log is not None
        assert log.append_segment == Segment(base_offset=10, path=empty)

    def test_starts_over_at_snapshot_with_empty_segment(self, tmp_path: Path) -> None:
        empty = write_segment(tmp_path, 28)
        snapshot = write_snapshot(tmp_path, 28, 5, voters_record(0, 1, 2))

        assert read_log(tmp_path) == EffectiveLog(
            end=LogEnd(last_offset=i64(27), partition_leader_epoch=i32(5)),
            voters=voters_record(0, 1, 2),
            snapshot=Snapshot(end_offset=i64(28), epoch=i32(5), path=snapshot),
            superseded=(),
            append_segment=Segment(base_offset=28, path=empty),
            incomplete_batch=None,
        )

    def test_starts_over_at_snapshot_without_segments(self, tmp_path: Path) -> None:
        write_snapshot(tmp_path, 28, 5, voters_record(0, 1, 2))

        log = read_log(tmp_path)

        assert log is not None
        assert log.end == LogEnd(last_offset=i64(27), partition_leader_epoch=i32(5))
        assert log.superseded == ()
        assert log.append_segment is None

    @pytest.mark.parametrize(
        ("last_offset", "epoch"),
        [
            pytest.param(20, 3, id="lower-epoch"),
            pytest.param(40, 3, id="lower-epoch-past-snapshot"),
            pytest.param(20, 5, id="same-epoch"),
        ],
    )
    def test_supersedes_segments_behind_snapshot(
        self,
        tmp_path: Path,
        last_offset: int,
        epoch: int,
    ) -> None:
        write_segment(
            tmp_path,
            0,
            voters_batch(voters_record(0, 1), offset=0, epoch=epoch),
            data_batch(offset=1, record_count=9, epoch=epoch),
        )
        write_segment(
            tmp_path,
            10,
            data_batch(offset=10, record_count=last_offset - 9, epoch=epoch),
        )
        write_snapshot(tmp_path, 28, 5, voters_record(0, 1, 2))

        log = read_log(tmp_path)

        assert log is not None
        assert log.end == LogEnd(last_offset=i64(27), partition_leader_epoch=i32(5))
        assert log.voters == voters_record(0, 1, 2)
        assert log.superseded == list_segments(tmp_path)
        assert log.append_segment is None

    def test_supersedes_non_empty_segment_at_snapshot_end(
        self,
        tmp_path: Path,
    ) -> None:
        write_segment(tmp_path, 28, data_batch(offset=28, epoch=3))
        write_snapshot(tmp_path, 28, 5, None)

        log = read_log(tmp_path)

        assert log is not None
        assert log.voters is None
        assert log.superseded == list_segments(tmp_path)
        assert log.append_segment is None

    def test_supersedes_older_segments_beside_empty_segment_at_snapshot_end(
        self,
        tmp_path: Path,
    ) -> None:
        old = write_segment(tmp_path, 0, data_batch(offset=0, epoch=3))
        empty = write_segment(tmp_path, 28)
        write_snapshot(tmp_path, 28, 5, None)

        log = read_log(tmp_path)

        assert log is not None
        assert log.superseded == (Segment(base_offset=0, path=old),)
        assert log.append_segment == Segment(base_offset=28, path=empty)

    def test_returns_no_voters_without_voters_in_log_or_snapshot(
        self,
        tmp_path: Path,
    ) -> None:
        write_segment(tmp_path, 0, data_batch(offset=0, record_count=10, epoch=2))
        write_snapshot(tmp_path, 5, 2, None)

        log = read_log(tmp_path)

        assert log is not None
        assert log.voters is None

    def test_returns_none_for_empty_segments_without_snapshot(
        self,
        tmp_path: Path,
    ) -> None:
        write_segment(tmp_path, 0)
        assert read_log(tmp_path) is None

    def test_returns_none_for_empty_segment_and_bootstrap_snapshot(
        self,
        tmp_path: Path,
    ) -> None:
        write_segment(tmp_path, 0)
        write_snapshot(tmp_path, 0, 0, voters_record(0, 1, 2))
        assert read_log(tmp_path) is None

    def test_raises_inconsistent_log_for_segment_after_end_of_log(
        self,
        tmp_path: Path,
    ) -> None:
        write_controller_log(tmp_path)
        write_segment(tmp_path, 15)
        with pytest.raises(
            InconsistentLog,
            match="segment 00000000000000000015.log starts after the last record",
        ):
            read_log(tmp_path)

    def test_raises_inconsistent_log_for_snapshot_after_end_with_lower_epoch(
        self,
        tmp_path: Path,
    ) -> None:
        write_controller_log(tmp_path)
        write_snapshot(tmp_path, 20, 2, voters_record(0, 1, 2))
        with pytest.raises(
            InconsistentLog,
            match="snapshot 00000000000000000020-0000000002.checkpoint ends after",
        ):
            read_log(tmp_path)

    def test_raises_corrupt_batch_for_corrupt_snapshot(self, tmp_path: Path) -> None:
        write_controller_log(tmp_path)
        path = write_snapshot(tmp_path, 5, 3, None)
        path.chmod(0o600)
        path.write_bytes(path.read_bytes()[:-1])
        with pytest.raises(CorruptBatch, match=str(path)):
            read_log(tmp_path)

    @pytest.mark.parametrize(
        "length",
        [
            pytest.param(10, id="inside-header"),
            pytest.param(17, id="up-to-magic"),
            pytest.param(18, id="past-magic"),
            pytest.param(-1, id="last-byte-missing"),
        ],
    )
    def test_truncates_incomplete_batch_at_end_of_log(
        self,
        tmp_path: Path,
        length: int,
    ) -> None:
        write_controller_log(tmp_path)
        complete = data_batch(offset=10, record_count=2, epoch=3)
        incomplete = data_batch(offset=12, epoch=4)[:length]
        path = write_segment(tmp_path, 10, complete, incomplete)

        log = read_log(tmp_path)

        assert log is not None
        assert log.end == LogEnd(last_offset=i64(11), partition_leader_epoch=i32(3))
        assert log.incomplete_batch == IncompleteBatch(
            segment=Segment(base_offset=10, path=path),
            position=len(complete),
            segment_size=len(complete) + len(incomplete),
        )

    def test_appends_to_segment_holding_only_incomplete_batch(
        self,
        tmp_path: Path,
    ) -> None:
        write_controller_log(tmp_path)
        path = write_segment(tmp_path, 10, data_batch(offset=10)[:30])

        log = read_log(tmp_path)

        assert log is not None
        assert log.end == LogEnd(last_offset=i64(9), partition_leader_epoch=i32(3))
        assert log.append_segment == Segment(base_offset=10, path=path)
        assert log.incomplete_batch == IncompleteBatch(
            segment=Segment(base_offset=10, path=path),
            position=0,
            segment_size=30,
        )

    def test_starts_over_at_snapshot_with_segment_holding_only_incomplete_batch(
        self,
        tmp_path: Path,
    ) -> None:
        path = write_segment(tmp_path, 28, data_batch(offset=28, epoch=5)[:30])
        write_snapshot(tmp_path, 28, 5, voters_record(0, 1, 2))

        log = read_log(tmp_path)

        assert log is not None
        assert log.superseded == ()
        assert log.append_segment == Segment(base_offset=28, path=path)
        assert log.incomplete_batch == IncompleteBatch(
            segment=Segment(base_offset=28, path=path),
            position=0,
            segment_size=30,
        )

    def test_compares_snapshot_with_log_after_truncating_incomplete_batch(
        self,
        tmp_path: Path,
    ) -> None:
        write_segment(
            tmp_path,
            0,
            voters_batch(voters_record(0, 1), offset=0, epoch=4),
            data_batch(offset=1, record_count=27, epoch=4),
            data_batch(offset=28, epoch=5)[:30],
        )
        write_snapshot(tmp_path, 28, 5, voters_record(0, 1, 2))

        log = read_log(tmp_path)

        assert log is not None
        assert log.end == LogEnd(last_offset=i64(27), partition_leader_epoch=i32(5))
        assert log.voters == voters_record(0, 1, 2)
        assert log.superseded == list_segments(tmp_path)
        assert log.incomplete_batch is None

    def test_raises_corrupt_batch_for_incomplete_batch_before_last_segment(
        self,
        tmp_path: Path,
    ) -> None:
        complete = voters_batch(voters_record(0, 1, 2), offset=0)
        path = write_segment(tmp_path, 0, complete, data_batch(offset=1)[:30])
        write_segment(tmp_path, 1, data_batch(offset=1))
        with pytest.raises(
            CorruptBatch,
            match=f"byte position {len(complete)} in {path}",
        ):
            read_log(tmp_path)

    @pytest.mark.parametrize(
        "tail",
        [
            pytest.param(corrupt_batch(offset=10), id="checksum-mismatch"),
            pytest.param(bytes(18), id="zeros"),
        ],
    )
    def test_raises_corrupt_batch_for_corrupt_batch_at_end_of_log(
        self,
        tmp_path: Path,
        tail: bytes,
    ) -> None:
        write_controller_log(tmp_path)
        path = write_segment(tmp_path, 10, tail)
        with pytest.raises(CorruptBatch, match=f"byte position 0 in {path}"):
            read_log(tmp_path)


class TestFindVoter:
    def test_returns_voter(self) -> None:
        assert find_voter(voters_record(0, 1, 2), BrokerId(1)) == voter(1)

    def test_raises_voter_not_found_listing_voters(self) -> None:
        with pytest.raises(
            VoterNotFound,
            match=r"^Node 3 is not a member of the current voter set \(0, 1, 2\)\.$",
        ):
            find_voter(voters_record(0, 1, 2), BrokerId(3))


class TestDemotionSequence:
    def test_removes_one_voter_at_a_time_starting_from_the_last(self) -> None:
        assert demotion_sequence(voters_record(1, 2, 3), BrokerId(1)) == (
            voters_record(1, 2),
            voters_record(1),
        )

    def test_puts_kept_voter_first(self) -> None:
        assert demotion_sequence(voters_record(1, 2, 3, 4, 5), BrokerId(3)) == (
            voters_record(3, 1, 2, 4),
            voters_record(3, 1, 2),
            voters_record(3, 1),
            voters_record(3),
        )

    def test_raises_nothing_to_recover_for_only_voter(self) -> None:
        with pytest.raises(NothingToRecover, match="already the only voter"):
            demotion_sequence(voters_record(1), BrokerId(1))

    def test_raises_voter_not_found_for_non_member(self) -> None:
        with pytest.raises(VoterNotFound):
            demotion_sequence(voters_record(1, 2, 3), BrokerId(4))


def test_encode_voters_batch_writes_single_record_control_batch() -> None:
    record = voters_record(0, 1)

    batch, size = read_batch(voters_batch(record, offset=1622, epoch=7), 0)

    assert size == batch.batch_length + 12
    assert batch.attributes & CONTROL_FLAG_MASK
    assert batch.base_offset == 1622
    assert batch.last_offset_delta == 0
    assert batch.partition_leader_epoch == 7
    assert batch.producer_id == -1
    [encoded] = batch.records
    assert encoded.offset == 1622
    assert encoded.timestamp == TIMESTAMP
    assert encoded.key == b"\x00\x00\x00\x06"
    assert encoded.value is not None
    decoded, _ = entity_reader(VotersRecord)(encoded.value, 0)
    assert decoded == record
