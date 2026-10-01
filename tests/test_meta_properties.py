import uuid

from pathlib import Path

import pytest

from kio.schema.types import BrokerId

from kraft_last_standing.errors import InvalidMetaProperties
from kraft_last_standing.errors import MetaPropertiesNotFound
from kraft_last_standing.meta_properties import MetaProperties
from kraft_last_standing.meta_properties import format_kafka_uuid
from kraft_last_standing.meta_properties import parse_kafka_uuid
from kraft_last_standing.meta_properties import parse_meta_properties
from kraft_last_standing.meta_properties import read_meta_properties

from .factories import CLUSTER_ID
from .factories import write_meta_properties

PATH = Path("meta.properties")
VALID = """\
#
#Thu Oct 01 12:00:00 UTC 2026
cluster.id=MkU3OEVBNTcwNTJENDM2Qg
directory.id=J8aAPcfLQt2bqs1JT_rMgQ
node.id=1
version=1
"""


class TestKafkaUuid:
    def test_roundtrips(self) -> None:
        value = uuid.uuid4()
        assert parse_kafka_uuid(format_kafka_uuid(value)) == value

    @pytest.mark.parametrize(
        "value", ["J8aAPcfLQt2bqs1JT_rMg", "J8aAPcfL?t2bqs1JT_rMgQ", ""]
    )
    def test_rejects_invalid_value(self, value: str) -> None:
        with pytest.raises(ValueError, match="Invalid Kafka Uuid"):
            parse_kafka_uuid(value)


class TestParseMetaProperties:
    def test_parses_valid_file(self) -> None:
        assert parse_meta_properties(PATH, VALID) == MetaProperties(
            cluster_id="MkU3OEVBNTcwNTJENDM2Qg",
            node_id=BrokerId(1),
            directory_id=parse_kafka_uuid("J8aAPcfLQt2bqs1JT_rMgQ"),
        )

    def test_ignores_blank_lines_and_surrounding_whitespace(self) -> None:
        text = "\n! comment\n  node.id = 1  \n" + VALID.replace("node.id=1\n", "")
        assert parse_meta_properties(PATH, text).node_id == 1

    @pytest.mark.parametrize(
        ("text", "reason"),
        [
            (VALID.replace("version=1", "version=0"), "expected version 1 .* '0'"),
            (VALID.replace("version=1\n", ""), "expected version 1 .* None"),
            (VALID.replace("node.id=1\n", ""), "missing property 'node.id'"),
            (VALID.replace("cluster.id", "cluster"), "missing property 'cluster.id'"),
            (
                VALID.replace("directory.id=J8aAPcfLQt2bqs1JT_rMgQ\n", ""),
                "missing property 'directory.id'",
            ),
            (VALID.replace("node.id=1", "node.id=one"), "invalid node.id 'one'"),
            (
                VALID.replace("node.id=1", "node.id=2147483648"),
                "invalid node.id '2147483648'",
            ),
            (
                VALID.replace("J8aAPcfLQt2bqs1JT_rMgQ", "J8aA"),
                "Invalid Kafka Uuid: 'J8aA'",
            ),
            (VALID + "garbage\n", "Expected key=value, got 'garbage'"),
        ],
    )
    def test_rejects_invalid_file(self, text: str, reason: str) -> None:
        with pytest.raises(
            InvalidMetaProperties, match=f"Failed parsing {PATH}: {reason}"
        ):
            parse_meta_properties(PATH, text)


class TestReadMetaProperties:
    def test_reads_file_in_log_directory(self, tmp_path: Path) -> None:
        directory = uuid.uuid4()
        write_meta_properties(tmp_path, node_id=3, directory=directory)
        assert read_meta_properties(tmp_path) == MetaProperties(
            cluster_id=CLUSTER_ID,
            node_id=BrokerId(3),
            directory_id=directory,
        )

    def test_raises_meta_properties_not_found_for_missing_file(
        self,
        tmp_path: Path,
    ) -> None:
        with pytest.raises(MetaPropertiesNotFound, match="meta.properties"):
            read_meta_properties(tmp_path)
