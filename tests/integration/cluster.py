import datetime
import io
import os
import secrets
import shlex
import socket
import subprocess
import textwrap
import time
import uuid

from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from kio.schema.add_raft_voter.v0.request import AddRaftVoterRequest
from kio.schema.add_raft_voter.v0.request import Listener
from kio.schema.add_raft_voter.v0.response import AddRaftVoterResponse
from kio.schema.describe_quorum.v2.request import DescribeQuorumRequest
from kio.schema.describe_quorum.v2.request import PartitionData as RequestPartition
from kio.schema.describe_quorum.v2.request import TopicData
from kio.schema.describe_quorum.v2.response import DescribeQuorumResponse
from kio.schema.describe_quorum.v2.response import PartitionData
from kio.schema.errors import ErrorCode
from kio.schema.request_header.v2.header import RequestHeader
from kio.schema.types import TopicName
from kio.serial import entity_reader
from kio.serial import entity_writer
from kio.static.primitive import i32
from kio.static.primitive import i32Timedelta
from kio.static.primitive import u16
from kio.static.protocol import HeaderV1ResponsePayload
from kio.static.protocol import HeaderV2RequestPayload

from kraft_last_standing.meta_properties import format_kafka_uuid

KAFKA_VERSION: Final = os.environ.get("KAFKA_VERSION", "4.3.1")
IMAGE: Final = f"apache/kafka:{KAFKA_VERSION}"
LISTENER_NAME: Final = "CONTROLLER"
LISTENER_PORT: Final = 9093

LOG4J2_CONFIG: Final = textwrap.dedent(
    """\
    Configuration:
      Appenders:
        Console:
          name: STDOUT
          PatternLayout:
            pattern: "[%d] %p %m (%c)%n"
      Loggers:
        Root:
          level: INFO
          AppenderRef:
            - ref: STDOUT
    """
)


def docker(*args: str) -> str:
    result = subprocess.run(
        ["docker", *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


@dataclass(frozen=True, slots=True, kw_only=True)
class Node:
    node_id: int
    container: str
    # Host directory mounted at the same path as the alias in the container.
    directory: Path

    @property
    def alias(self) -> str:
        return f"controller-{self.node_id}"

    @property
    def mount(self) -> str:
        return f"/node-{self.node_id}"

    @property
    def log_dir(self) -> Path:
        return self.directory / "data"

    @property
    def metadata_dir(self) -> Path:
        return self.log_dir / "__cluster_metadata-0"


@dataclass(frozen=True, slots=True, kw_only=True)
class Cluster:
    cluster_id: str
    network: str
    voters: tuple[Node, ...]
    observer: Node

    @property
    def nodes(self) -> tuple[Node, ...]:
        return (*self.voters, self.observer)


def wait_until[T](
    what: str,
    attempt: Callable[[], T | None],
    timeout: float = 120.0,
) -> T:
    deadline = time.monotonic() + timeout
    while (result := attempt()) is None:
        if time.monotonic() > deadline:
            raise TimeoutError(f"Timed out waiting for {what}.")
        time.sleep(0.5)
    return result


def _server_properties(node: Node, voters: Sequence[Node]) -> str:
    bootstrap = ",".join(f"{voter.alias}:{LISTENER_PORT}" for voter in voters)
    # The metadata.* settings roll segments, take snapshots and delete segments behind
    # them within seconds, rather than hours.
    return textwrap.dedent(
        f"""\
        process.roles=controller
        node.id={node.node_id}
        listeners={LISTENER_NAME}://0.0.0.0:{LISTENER_PORT}
        advertised.listeners={LISTENER_NAME}://{node.alias}:{LISTENER_PORT}
        controller.listener.names={LISTENER_NAME}
        listener.security.protocol.map={LISTENER_NAME}:PLAINTEXT
        controller.quorum.bootstrap.servers={bootstrap}
        log.dirs={node.mount}/data
        metadata.log.segment.ms=2000
        metadata.log.max.snapshot.interval.ms=2000
        metadata.max.retention.ms=4000
        internal.metadata.delete.delay.millis=1000
        """
    )


def _user() -> str:
    # Running as the user of the test process keeps files in bind mounts owned by it.
    return f"{os.getuid()}:{os.getgid()}"


def _volume(node: Node) -> str:
    return f"{node.directory}:{node.mount}"


def _format_storage(cluster: Cluster) -> None:
    directory_ids = {
        voter.node_id: format_kafka_uuid(uuid.uuid4()) for voter in cluster.voters
    }
    initial_controllers = ",".join(
        f"{voter.node_id}@{voter.alias}:{LISTENER_PORT}:{directory_ids[voter.node_id]}"
        for voter in cluster.voters
    )

    def format_command(node: Node, *quorum_args: str) -> str:
        return shlex.join(
            (
                "/opt/kafka/bin/kafka-storage.sh",
                "format",
                f"--cluster-id={cluster.cluster_id}",
                *quorum_args,
                f"--config={node.mount}/server.properties",
            )
        )

    commands = (
        *(
            format_command(voter, f"--initial-controllers={initial_controllers}")
            for voter in cluster.voters
        ),
        format_command(cluster.observer, "--no-initial-controllers"),
    )
    docker(
        "run",
        "--rm",
        f"--user={_user()}",
        "--env=LOG_DIR=/tmp",
        *(f"--volume={_volume(node)}" for node in cluster.nodes),
        "--entrypoint=/bin/bash",
        IMAGE,
        "-c",
        " && ".join(commands),
    )


def _run(node: Node, network: str) -> None:
    docker(
        "run",
        "--detach",
        f"--name={node.container}",
        f"--network={network}",
        f"--network-alias={node.alias}",
        f"--user={_user()}",
        "--env=LOG_DIR=/tmp",
        "--env=KAFKA_LOG4J_OPTS="
        f"-Dlog4j2.configurationFile=file:{node.mount}/log4j2.yaml",
        "--env=KAFKA_HEAP_OPTS=-Xmx256m",
        f"--volume={_volume(node)}",
        f"--publish=127.0.0.1::{LISTENER_PORT}",
        "--entrypoint=/opt/kafka/bin/kafka-server-start.sh",
        IMAGE,
        f"{node.mount}/server.properties",
    )


def start(*nodes: Node) -> None:
    docker("start", *(node.container for node in nodes))


def stop(*nodes: Node) -> None:
    docker("stop", *(node.container for node in nodes))


def kill(*nodes: Node) -> None:
    docker("kill", *(node.container for node in nodes))


def _save_logs(node: Node) -> None:
    result = subprocess.run(
        ["docker", "logs", node.container],
        capture_output=True,
        text=True,
        check=False,
    )
    (node.directory / "container.log").write_text(result.stdout + result.stderr)


def _node(prefix: str, base: Path, node_id: int) -> Node:
    return Node(
        node_id=node_id,
        container=f"{prefix}-controller-{node_id}",
        directory=base / f"node-{node_id}",
    )


def run_cluster(base: Path) -> Iterator[Cluster]:
    """
    Start three voters and one observer, and tear them down when the generator is
    closed. Container logs are saved in each node's directory.
    """
    prefix = f"kraft-last-standing-{secrets.token_hex(4)}"
    voters = tuple(_node(prefix, base, node_id) for node_id in (0, 1, 2))
    cluster = Cluster(
        cluster_id=format_kafka_uuid(uuid.uuid4()),
        network=prefix,
        voters=voters,
        observer=_node(prefix, base, 3),
    )
    for node in cluster.nodes:
        node.directory.mkdir(parents=True)
        (node.directory / "server.properties").write_text(
            _server_properties(node, voters)
        )
        (node.directory / "log4j2.yaml").write_text(LOG4J2_CONFIG)

    _format_storage(cluster)
    docker("network", "create", cluster.network)
    try:
        for node in cluster.nodes:
            _run(node, cluster.network)
        yield cluster
    finally:
        for node in cluster.nodes:
            _save_logs(node)
        subprocess.run(
            ["docker", "rm", "--force", *(node.container for node in cluster.nodes)],
            capture_output=True,
            check=False,
        )
        docker("network", "rm", cluster.network)


def _host_port(node: Node) -> int | None:
    result = subprocess.run(
        ["docker", "port", node.container, f"{LISTENER_PORT}/tcp"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0 or not result.stdout:
        return None
    _, _, port = result.stdout.splitlines()[0].rpartition(":")
    return int(port)


def _receive(connection: socket.socket, size: int) -> bytes:
    buffer = bytearray()
    while len(buffer) < size:
        chunk = connection.recv(size - len(buffer))
        if not chunk:
            raise ConnectionError("Connection closed while reading response.")
        buffer.extend(chunk)
    return bytes(buffer)


def request[R: HeaderV1ResponsePayload](
    port: int,
    payload: HeaderV2RequestPayload,
    response_type: type[R],
) -> R:
    correlation_id = i32(secrets.randbelow(i32.__high__ + 1))
    header = RequestHeader(
        request_api_key=payload.__api_key__,
        request_api_version=payload.__version__,
        correlation_id=correlation_id,
        client_id="kraft-last-standing-tests",
    )
    with io.BytesIO() as buffer:
        entity_writer(RequestHeader)(buffer, header)
        entity_writer(type(payload))(buffer, payload)
        message = buffer.getvalue()

    with socket.create_connection(("127.0.0.1", port), timeout=10) as connection:
        connection.sendall(len(message).to_bytes(4, "big") + message)
        size = int.from_bytes(_receive(connection, 4), "big")
        response = _receive(connection, size)

    response_header, header_size = entity_reader(response_type.__header_schema__)(
        response,
        0,
    )
    assert response_header.correlation_id == correlation_id
    result, result_size = entity_reader(response_type)(response, header_size)
    assert header_size + result_size == len(response)
    return result


def _describe_quorum(port: int) -> PartitionData | None:
    try:
        response = request(
            port,
            DescribeQuorumRequest(
                topics=(
                    TopicData(
                        topic_name=TopicName("__cluster_metadata"),
                        partitions=(RequestPartition(partition_index=i32(0)),),
                    ),
                ),
            ),
            DescribeQuorumResponse,
        )
    except OSError:
        return None
    [topic] = response.topics
    [partition] = topic.partitions
    return partition if partition.error_code is ErrorCode.none else None


def describe_quorum(nodes: Iterable[Node]) -> PartitionData | None:
    """
    Describe the quorum as seen by the leader, which is the only node that answers.
    """
    for node in nodes:
        port = _host_port(node)
        if port is None:
            continue
        if (partition := _describe_quorum(port)) is not None:
            return partition
    return None


def wait_for_quorum(
    nodes: Sequence[Node],
    voters: set[int],
    observers: set[int],
) -> PartitionData:
    """
    Wait for the given voters and observers, all caught up to the high watermark.
    Voters are listed as soon as the voter set is known, which is before they have
    replicated anything.
    """

    def attempt() -> PartitionData | None:
        partition = describe_quorum(nodes)
        if partition is None or partition.high_watermark <= 0:
            return None
        replicas = (*partition.current_voters, *partition.observers)
        current_voters = {voter.replica_id for voter in partition.current_voters}
        current_observers = {observer.replica_id for observer in partition.observers}
        if (current_voters, current_observers) != (voters, observers) or any(
            replica.log_end_offset < partition.high_watermark for replica in replicas
        ):
            return None
        return partition

    return wait_until(f"voters {voters} and observers {observers}", attempt)


def add_voter(
    cluster: Cluster,
    leader: Node,
    node: Node,
    directory_id: uuid.UUID | None,
) -> None:
    """
    Promote an observer to voter. The leader answers REQUEST_TIMED_OUT until the
    observer has caught up, see KAFKA-17147, so keep retrying.
    """

    def attempt() -> ErrorCode | None:
        port = _host_port(leader)
        assert port is not None
        response = request(
            port,
            AddRaftVoterRequest(
                cluster_id=cluster.cluster_id,
                timeout=i32Timedelta.parse(datetime.timedelta(seconds=30)),
                voter_id=i32(node.node_id),
                voter_directory_id=directory_id,
                listeners=(
                    Listener(
                        name=LISTENER_NAME,
                        host=node.alias,
                        port=u16(LISTENER_PORT),
                    ),
                ),
            ),
            AddRaftVoterResponse,
        )
        if response.error_code is ErrorCode.request_timed_out:
            return None
        return response.error_code

    assert wait_until("voter to be added", attempt) is ErrorCode.none
