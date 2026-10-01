import itertools

from collections.abc import Iterator
from pathlib import Path

import pytest

from kraft_last_standing import cli
from kraft_last_standing.errors import CorruptBatch
from kraft_last_standing.files import FileAttributes
from kraft_last_standing.metadata_log import list_segments
from kraft_last_standing.metadata_log import read_batches
from kraft_last_standing.metadata_log import summarize
from kraft_last_standing.probe import probe

from .cluster import Cluster
from .cluster import Node
from .cluster import add_voter
from .cluster import describe_quorum
from .cluster import kill
from .cluster import run_cluster
from .cluster import start
from .cluster import stop
from .cluster import wait_for_quorum
from .cluster import wait_until

pytestmark = pytest.mark.integration


@pytest.fixture
def cluster(tmp_path: Path) -> Iterator[Cluster]:
    yield from run_cluster(tmp_path / "cluster")


@pytest.fixture
def running_cluster(cluster: Cluster) -> Cluster:
    wait_for_quorum(cluster.nodes, voters={0, 1, 2}, observers={3})
    return cluster


def _recover(node: Node, backup_dir: Path, *args: str) -> None:
    exit_code = cli.main(
        [
            "recover",
            f"--metadata-dir={node.metadata_dir}",
            f"--backup-dir={backup_dir}",
            "--yes",
            *args,
        ]
    )
    assert exit_code == 0


def _last_offset(metadata_dir: Path) -> int:
    summary = summarize(read_batches(list_segments(metadata_dir)))
    assert summary is not None
    return summary.end.last_offset


def _assert_consistent_attributes(metadata_dir: Path) -> None:
    segments = list_segments(metadata_dir)
    assert len({FileAttributes.of(segment.path) for segment in segments}) == 1


def test_recover_last_standing(running_cluster: Cluster, tmp_path: Path) -> None:
    cluster = running_cluster
    survivor = cluster.voters[0]
    wait_until(
        "the log to span several segments",
        lambda: True if len(list_segments(survivor.metadata_dir)) > 1 else None,
    )
    kill(*cluster.voters)

    _recover(survivor, tmp_path / "backup")

    _assert_consistent_attributes(survivor.metadata_dir)
    start(survivor)
    quorum = wait_for_quorum([survivor], voters={0}, observers={3})

    [observer] = quorum.observers
    add_voter(
        cluster,
        leader=survivor,
        node=cluster.observer,
        directory_id=observer.replica_directory_id,
    )
    wait_for_quorum([survivor, cluster.observer], voters={0, 3}, observers=set())


def test_lagging_observer_does_not_skip_records(
    running_cluster: Cluster,
    tmp_path: Path,
) -> None:
    cluster = running_cluster
    survivor = cluster.voters[0]
    stop(cluster.observer)
    observer_last_offset = _last_offset(cluster.observer.metadata_dir)

    # The active controller appends a NoOpRecord every metadata.max.idle.interval.ms,
    # 500 ms by default, so the voters' logs move ahead of the stopped observer.
    def voters_ahead() -> bool | None:
        quorum = describe_quorum(cluster.voters)
        if quorum is None or quorum.high_watermark <= observer_last_offset + 10:
            return None
        return True

    wait_until("voters to move ahead of the observer", voters_ahead)
    kill(*cluster.voters)
    _recover(survivor, tmp_path / "backup")
    recovered_last_offset = _last_offset(survivor.metadata_dir)

    start(survivor, cluster.observer)

    def observer_caught_up() -> bool | None:
        quorum = describe_quorum([survivor])
        if quorum is None:
            return None
        caught_up = any(
            observer.replica_id == cluster.observer.node_id
            and observer.log_end_offset > recovered_last_offset
            for observer in quorum.observers
        )
        return True if caught_up else None

    wait_until("observer to catch up", observer_caught_up)
    stop(cluster.observer)

    batches = tuple(read_batches(list_segments(cluster.observer.metadata_dir)))
    for previous, batch in itertools.pairwise(batches):
        assert (
            batch.base_offset == previous.base_offset + previous.last_offset_delta + 1
        )
    summary = summarize(batches)
    assert summary is not None
    assert summary.voters is not None
    assert [voter.voter_id for voter in summary.voters.voters] == [0]


def test_recover_with_copied_log(running_cluster: Cluster, tmp_path: Path) -> None:
    cluster = running_cluster
    survivor = cluster.voters[0]
    kill(*cluster.voters)
    stop(cluster.observer)
    for node in (survivor, cluster.observer):
        assert probe(node.metadata_dir).voter_ids == (0, 1, 2)

    _recover(
        survivor,
        tmp_path / "backup",
        f"--copy-log-from={cluster.observer.metadata_dir}",
    )

    _assert_consistent_attributes(survivor.metadata_dir)
    start(survivor, cluster.observer)
    wait_for_quorum([survivor], voters={0}, observers={3})


def _voters_only_in_snapshot(metadata_dir: Path) -> bool | None:
    try:
        segments = list_segments(metadata_dir)
        summary = summarize(read_batches(segments))
    except (CorruptBatch, FileNotFoundError):
        # Read while Kafka appends to, or deletes, a segment.
        return None
    if not segments or segments[0].base_offset == 0:
        return None
    return True if summary is not None and summary.voters is None else None


def test_recover_with_voters_only_in_snapshot(
    running_cluster: Cluster,
    tmp_path: Path,
) -> None:
    cluster = running_cluster
    survivor = cluster.voters[0]
    # Kafka deletes segments behind snapshots once a minute, a hard-coded interval.
    wait_until(
        "segments holding the voter set to be deleted",
        lambda: _voters_only_in_snapshot(survivor.metadata_dir),
        timeout=180,
    )
    kill(*cluster.voters)
    result = probe(survivor.metadata_dir)
    assert result.snapshot is not None
    assert result.voter_ids == (0, 1, 2)

    _recover(survivor, tmp_path / "backup")

    _assert_consistent_attributes(survivor.metadata_dir)
    start(survivor)
    wait_for_quorum([survivor], voters={0}, observers={3})


def test_recover_with_copied_snapshot(
    running_cluster: Cluster,
    tmp_path: Path,
) -> None:
    cluster = running_cluster
    survivor, source, _ = cluster.voters
    wait_until(
        "segments holding the voter set to be deleted",
        lambda: _voters_only_in_snapshot(source.metadata_dir),
        timeout=180,
    )
    kill(*cluster.voters)

    _recover(
        survivor,
        tmp_path / "backup",
        f"--copy-log-from={source.metadata_dir}",
    )

    _assert_consistent_attributes(survivor.metadata_dir)
    start(survivor)
    wait_for_quorum([survivor], voters={0}, observers={3})
