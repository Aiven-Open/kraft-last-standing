import json

from pathlib import Path

import pytest

from kraft_last_standing.errors import InvalidQuorumState
from kraft_last_standing.quorum_state import encode_unattached
from kraft_last_standing.quorum_state import parse_quorum_state_epoch
from kraft_last_standing.quorum_state import read_quorum_state_epoch

from .factories import write_quorum_state

PATH = Path("quorum-state")


class TestParseQuorumStateEpoch:
    def test_parses_epoch(self) -> None:
        text = (
            '{"leaderId":2,"leaderEpoch":7,"votedId":0,'
            '"votedDirectoryId":"J8aAPcfLQt2bqs1JT_rMgQ","data_version":1}'
        )
        assert parse_quorum_state_epoch(PATH, text) == 7

    @pytest.mark.parametrize(
        "text",
        [
            pytest.param("", id="empty"),
            pytest.param("[7]", id="not-an-object"),
            pytest.param('{"leaderId":2}', id="missing-epoch"),
            pytest.param('{"leaderEpoch":"7"}', id="string-epoch"),
        ],
    )
    def test_rejects_invalid_file(self, text: str) -> None:
        with pytest.raises(InvalidQuorumState, match=f"leader epoch from {PATH}"):
            parse_quorum_state_epoch(PATH, text)


class TestReadQuorumStateEpoch:
    def test_reads_file_in_metadata_directory(self, tmp_path: Path) -> None:
        write_quorum_state(tmp_path, epoch=7, voted_id=0)
        assert read_quorum_state_epoch(tmp_path) == 7

    def test_returns_zero_for_missing_file(self, tmp_path: Path) -> None:
        assert read_quorum_state_epoch(tmp_path) == 0


def test_encode_unattached_writes_epoch_without_leader_or_vote() -> None:
    encoded = encode_unattached(7)
    assert json.loads(encoded) == {
        "leaderId": -1,
        "leaderEpoch": 7,
        "votedId": -1,
        "votedDirectoryId": "AAAAAAAAAAAAAAAAAAAAAA",
        "data_version": 1,
    }
    assert parse_quorum_state_epoch(PATH, encoded.decode()) == 7
