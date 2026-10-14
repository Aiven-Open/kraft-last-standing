"""
The ``quorum-state`` file, where Kafka keeps the election state of a replica: its epoch,
the leader it knows of in that epoch, and the candidate it voted for.
"""

import json
import uuid

from pathlib import Path

from .errors import InvalidQuorumState
from .meta_properties import format_kafka_uuid


def quorum_state_path(metadata_dir: Path) -> Path:
    return metadata_dir / "quorum-state"


def parse_quorum_state_epoch(path: Path, text: str) -> int:
    try:
        epoch = json.loads(text)["leaderEpoch"]
    except (ValueError, KeyError, TypeError) as exception:
        raise InvalidQuorumState(path) from exception
    if not isinstance(epoch, int):
        raise InvalidQuorumState(path)
    return epoch


def read_quorum_state_epoch(metadata_dir: Path) -> int:
    path = quorum_state_path(metadata_dir)
    try:
        text = path.read_text()
    except FileNotFoundError:
        # Kafka starts in epoch 0 without the file.
        # https://github.com/apache/kafka/blob/26b251a451ce941d3d7a55e6487bcb7f16b5ad48/raft/src/main/java/org/apache/kafka/raft/QuorumState.java#L130-L139
        return 0
    return parse_quorum_state_epoch(path, text)


def encode_unattached(epoch: int) -> bytes:
    """
    The election state of a replica that knows of no leader in ``epoch``, and hasn't
    voted in it, in the format Kafka writes for dynamic quorums.
    """
    return json.dumps(
        {
            "leaderId": -1,
            "leaderEpoch": epoch,
            "votedId": -1,
            "votedDirectoryId": format_kafka_uuid(uuid.UUID(int=0)),
            "data_version": 1,
        },
        separators=(",", ":"),
    ).encode()
