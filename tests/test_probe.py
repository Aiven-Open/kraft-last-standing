from pathlib import Path

import pytest

from kio.schema.types import BrokerId
from kio.static.primitive import i32
from kio.static.primitive import i64

from kraft_last_standing.errors import InvalidMetaProperties
from kraft_last_standing.metadata_log import LogEnd
from kraft_last_standing.metadata_log import Snapshot
from kraft_last_standing.probe import ProbeResult
from kraft_last_standing.probe import most_progressed_first
from kraft_last_standing.probe import probe

from .factories import data_batch
from .factories import directory_id
from .factories import voters_batch
from .factories import voters_record
from .factories import write_controller_log
from .factories import write_meta_properties
from .factories import write_segment
from .factories import write_snapshot


class TestProbe:
    def test_returns_node_id_log_end_and_voters(self, tmp_path: Path) -> None:
        metadata_dir = tmp_path / "__cluster_metadata-0"
        write_controller_log(metadata_dir)
        write_meta_properties(tmp_path, node_id=2, directory=directory_id(2))

        assert probe(metadata_dir) == ProbeResult(
            directory=metadata_dir,
            node_id=BrokerId(2),
            log_end=LogEnd(last_offset=i64(9), partition_leader_epoch=i32(3)),
            snapshot=None,
            voter_ids=(BrokerId(0), BrokerId(1), BrokerId(2)),
        )

    def test_returns_log_end_and_voters_of_snapshot_ahead_of_segments(
        self,
        tmp_path: Path,
    ) -> None:
        write_controller_log(tmp_path)
        snapshot = write_snapshot(tmp_path, 28, 5, voters_record(0, 1, 2, 3))

        result = probe(tmp_path)

        assert result.log_end == LogEnd(
            last_offset=i64(27), partition_leader_epoch=i32(5)
        )
        assert result.snapshot == Snapshot(
            end_offset=i64(28),
            epoch=i32(5),
            path=snapshot,
        )
        assert result.voter_ids == (BrokerId(0), BrokerId(1), BrokerId(2), BrokerId(3))

    def test_returns_no_node_id_without_meta_properties(self, tmp_path: Path) -> None:
        write_segment(tmp_path, 0, voters_batch(voters_record(), offset=0))
        result = probe(tmp_path)
        assert result.node_id is None
        assert result.voter_ids == ()

    def test_raises_invalid_meta_properties(self, tmp_path: Path) -> None:
        metadata_dir = tmp_path / "__cluster_metadata-0"
        write_controller_log(metadata_dir)
        (tmp_path / "meta.properties").write_text("version=0\n")
        with pytest.raises(InvalidMetaProperties):
            probe(metadata_dir)

    def test_returns_no_voters_without_voters_record(self, tmp_path: Path) -> None:
        write_segment(tmp_path, 0, data_batch(offset=0, record_count=3))
        result = probe(tmp_path)
        assert result.log_end == LogEnd(
            last_offset=i64(2), partition_leader_epoch=i32(1)
        )
        assert result.voter_ids is None

    def test_returns_no_log_end_without_batches(self, tmp_path: Path) -> None:
        write_snapshot(tmp_path, 0, 0, voters_record(0))
        result = probe(tmp_path)
        assert result.log_end is None
        assert result.snapshot is None
        assert result.voter_ids is None


def test_most_progressed_first_sorts_by_leader_epoch_then_last_offset() -> None:
    def result(name: str, log_end: tuple[int, int] | None) -> ProbeResult:
        return ProbeResult(
            directory=Path(name),
            node_id=None,
            log_end=(
                None
                if log_end is None
                else LogEnd(
                    partition_leader_epoch=i32(log_end[0]),
                    last_offset=i64(log_end[1]),
                )
            ),
            snapshot=None,
            voter_ids=None,
        )

    results = [
        result("a", None),
        result("b", (1, 5)),
        result("c", (1, 1621)),
        result("d", (1, 0)),
        result("e", (2, 3)),
    ]

    ordered = sorted(results, key=most_progressed_first)

    assert [result.directory.name for result in ordered] == ["e", "c", "b", "d", "a"]
