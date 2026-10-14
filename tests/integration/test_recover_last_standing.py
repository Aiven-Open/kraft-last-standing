import itertools
import json

from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Sequence
from pathlib import Path

import pytest

from kraft_last_standing import cli
from kraft_last_standing.errors import CorruptBatch
from kraft_last_standing.files import FileAttributes
from kraft_last_standing.meta_properties import format_kafka_uuid
from kraft_last_standing.meta_properties import read_meta_properties
from kraft_last_standing.metadata_log import list_segments
from kraft_last_standing.metadata_log import read_batches
from kraft_last_standing.metadata_log import summarize
from kraft_last_standing.probe import most_progressed_first
from kraft_last_standing.probe import probe
from kraft_last_standing.quorum_state import quorum_state_path
from kraft_last_standing.quorum_state import read_quorum_state_epoch

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


def _most_progressed(nodes: Iterable[Node]) -> Node:
    # Recovering from a less progressed copy than the running observer's makes the new
    # leader ask the observer to truncate below its high watermark, which is fatal.
    return min(nodes, key=lambda node: most_progressed_first(probe(node.metadata_dir)))


def test_recover_last_standing(running_cluster: Cluster, tmp_path: Path) -> None:
    cluster = running_cluster
    wait_until(
        "the logs to span several segments",
        lambda: (
            True
            if all(
                len(list_segments(voter.metadata_dir)) > 1 for voter in cluster.voters
            )
            else None
        ),
    )
    kill(*cluster.voters)
    survivor = _most_progressed(cluster.voters)

    _recover(survivor, tmp_path / "backup")

    _assert_consistent_attributes(survivor.metadata_dir)
    start(survivor)
    quorum = wait_for_quorum([survivor], voters={survivor.node_id}, observers={3})

    [observer] = quorum.observers
    add_voter(
        cluster,
        leader=survivor,
        node=cluster.observer,
        directory_id=observer.replica_directory_id,
    )
    wait_for_quorum(
        [survivor, cluster.observer],
        voters={survivor.node_id, cluster.observer.node_id},
        observers=set(),
    )


def _vote_for_itself(node: Node) -> int:
    """
    Make the stopped node's quorum-state record a vote for itself in its epoch, like a
    candidate that lost the election, or never learned its outcome. Returns the epoch.
    """
    epoch = read_quorum_state_epoch(node.metadata_dir)
    directory_id = read_meta_properties(node.log_dir).directory_id
    quorum_state_path(node.metadata_dir).write_text(
        json.dumps(
            {
                "leaderId": -1,
                "leaderEpoch": epoch,
                "votedId": node.node_id,
                "votedDirectoryId": format_kafka_uuid(directory_id),
                "data_version": 1,
            }
        )
    )
    return epoch


def test_recover_voter_that_voted_for_itself(
    running_cluster: Cluster,
    tmp_path: Path,
) -> None:
    cluster = running_cluster
    kill(*cluster.voters)
    survivor = _most_progressed(cluster.voters)
    epoch = _vote_for_itself(survivor)

    _recover(survivor, tmp_path / "backup")

    start(survivor)
    quorum = wait_for_quorum([survivor], voters={survivor.node_id}, observers={3})
    assert quorum.leader_epoch > epoch


def test_lagging_observer_does_not_skip_records(
    running_cluster: Cluster,
    tmp_path: Path,
) -> None:
    cluster = running_cluster
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
    survivor = _most_progressed(cluster.voters)
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
    assert [voter.voter_id for voter in summary.voters.voters] == [survivor.node_id]


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


def _voters_only_in_snapshot(metadata_dir: Path) -> bool:
    try:
        segments = list_segments(metadata_dir)
        summary = summarize(read_batches(segments))
    except (CorruptBatch, FileNotFoundError):
        # Read while Kafka appends to, or deletes, a segment.
        return False
    if not segments or segments[0].base_offset == 0:
        return False
    return summary is not None and summary.voters is None


def _wait_for_voters_only_in_snapshots(nodes: Sequence[Node]) -> None:
    # Kafka deletes segments behind snapshots once a minute, a hard-coded interval.
    wait_until(
        "segments holding the voter set to be deleted",
        lambda: (
            True
            if all(_voters_only_in_snapshot(node.metadata_dir) for node in nodes)
            else None
        ),
        timeout=180,
    )


def test_recover_with_voters_only_in_snapshot(
    running_cluster: Cluster,
    tmp_path: Path,
) -> None:
    cluster = running_cluster
    _wait_for_voters_only_in_snapshots(cluster.voters)
    kill(*cluster.voters)
    survivor = _most_progressed(cluster.voters)
    result = probe(survivor.metadata_dir)
    assert result.snapshot is not None
    assert result.voter_ids == (0, 1, 2)

    _recover(survivor, tmp_path / "backup")

    _assert_consistent_attributes(survivor.metadata_dir)
    start(survivor)
    wait_for_quorum([survivor], voters={survivor.node_id}, observers={3})


def test_recover_with_copied_snapshot(
    running_cluster: Cluster,
    tmp_path: Path,
) -> None:
    cluster = running_cluster
    _wait_for_voters_only_in_snapshots(cluster.voters)
    kill(*cluster.voters)
    source = _most_progressed(cluster.voters)
    survivor = next(voter for voter in cluster.voters if voter != source)

    _recover(
        survivor,
        tmp_path / "backup",
        f"--copy-log-from={source.metadata_dir}",
    )

    _assert_consistent_attributes(survivor.metadata_dir)
    start(survivor)
    wait_for_quorum([survivor], voters={survivor.node_id}, observers={3})
