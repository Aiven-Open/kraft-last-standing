"""
Parsing of the ``meta.properties`` file that ``kafka-storage.sh format`` writes to the
root of each log directory.
"""

import base64
import binascii
import uuid

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from kio.schema.types import BrokerId

from .errors import InvalidMetaProperties
from .errors import MetaPropertiesNotFound


@dataclass(frozen=True, slots=True, kw_only=True)
class MetaProperties:
    cluster_id: str
    node_id: BrokerId
    directory_id: uuid.UUID


def parse_kafka_uuid(value: str) -> uuid.UUID:
    """
    Parse the string representation of a Kafka Uuid, which is the URL-safe base64
    encoding of its 16 bytes, without padding.

    >>> parse_kafka_uuid("AAAAAAAAAAAAAAAAAAAAAA")
    UUID('00000000-0000-0000-0000-000000000000')
    """
    padded = value + "=" * (-len(value) % 4)
    try:
        raw = base64.urlsafe_b64decode(padded)
    except binascii.Error as exception:
        raise ValueError(f"Invalid Kafka Uuid: {value!r}") from exception
    if len(raw) != 16:
        raise ValueError(f"Invalid Kafka Uuid: {value!r}")
    return uuid.UUID(bytes=raw)


def format_kafka_uuid(value: uuid.UUID) -> str:
    """
    >>> format_kafka_uuid(uuid.UUID(int=1))
    'AAAAAAAAAAAAAAAAAAAAAQ'
    """
    return base64.urlsafe_b64encode(value.bytes).rstrip(b"=").decode()


def _parse_properties(text: str) -> Iterator[tuple[str, str]]:
    # This handles the subset of the Java properties format that Kafka writes to
    # meta.properties: comments, and key=value pairs that need no escaping.
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith(("#", "!")):
            continue
        key, separator, value = stripped.partition("=")
        if not separator:
            raise ValueError(f"Expected key=value, got {stripped!r}")
        yield key.strip(), value.strip()


def parse_meta_properties(path: Path, text: str) -> MetaProperties:
    try:
        properties = dict(_parse_properties(text))
    except ValueError as exception:
        raise InvalidMetaProperties(path, str(exception)) from exception

    if (version := properties.get("version")) != "1":
        raise InvalidMetaProperties(
            path, f"expected version 1 (KRaft), found {version!r}"
        )

    try:
        cluster_id = properties["cluster.id"]
        raw_node_id = properties["node.id"]
        raw_directory_id = properties["directory.id"]
    except KeyError as exception:
        raise InvalidMetaProperties(
            path, f"missing property {exception.args[0]!r}"
        ) from exception

    try:
        node_id = BrokerId.parse(int(raw_node_id))
    except (ValueError, TypeError) as exception:
        raise InvalidMetaProperties(
            path, f"invalid node.id {raw_node_id!r}"
        ) from exception

    try:
        directory_id = parse_kafka_uuid(raw_directory_id)
    except ValueError as exception:
        raise InvalidMetaProperties(path, str(exception)) from exception

    return MetaProperties(
        cluster_id=cluster_id,
        node_id=node_id,
        directory_id=directory_id,
    )


def read_meta_properties(log_directory: Path) -> MetaProperties:
    path = log_directory / "meta.properties"
    try:
        text = path.read_text()
    except FileNotFoundError:
        raise MetaPropertiesNotFound(path) from None
    return parse_meta_properties(path, text)
