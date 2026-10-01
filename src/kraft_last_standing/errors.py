import uuid

from collections.abc import Iterable
from pathlib import Path

from kio.schema.types import BrokerId


class KRaftLastStandingError(Exception):
    """Base class for errors that abort an operation before anything is modified."""


class MetaPropertiesNotFound(KRaftLastStandingError):
    def __init__(self, path: Path) -> None:
        super().__init__(f"Found no {path}.")


class InvalidMetaProperties(KRaftLastStandingError):
    def __init__(self, path: Path, reason: str) -> None:
        super().__init__(f"Failed parsing {path}: {reason}")


class InvalidSegmentName(KRaftLastStandingError):
    def __init__(self, path: Path) -> None:
        super().__init__(
            f"Log segment {path} does not have a numeric base offset as its name."
        )


class InvalidSnapshotName(KRaftLastStandingError):
    def __init__(self, path: Path) -> None:
        super().__init__(
            f"Snapshot {path} is not named after an end offset and leader epoch, like "
            f"00000000000000001622-0000000005.checkpoint."
        )


class NoLogSegments(KRaftLastStandingError):
    def __init__(self, directory: Path) -> None:
        super().__init__(f"The metadata log in {directory} is empty.")


class InconsistentLog(KRaftLastStandingError):
    def __init__(self, directory: Path, reason: str) -> None:
        super().__init__(
            f"The metadata log in {directory} is in a state Kafka doesn't leave it in: "
            f"{reason}"
        )


class CorruptBatch(KRaftLastStandingError):
    def __init__(self, path: Path, position: int) -> None:
        super().__init__(
            f"Failed reading record batch at byte position {position} in {path}. The "
            f"metadata log is corrupt, use another copy of it."
        )


class SegmentChanged(KRaftLastStandingError):
    def __init__(self, path: Path) -> None:
        super().__init__(
            f"Log segment {path} changed after planning. Make sure that the Kafka "
            f"process is stopped."
        )


class VotersRecordNotFound(KRaftLastStandingError):
    def __init__(self, directory: Path) -> None:
        super().__init__(
            f"Found no VotersRecord in the log segments or the latest snapshot in "
            f"{directory}. Static quorums, which keep the voter set in the "
            f"configuration, are not supported."
        )


class CopiedVotersRecordNotFound(KRaftLastStandingError):
    def __init__(self, directory: Path) -> None:
        super().__init__(
            f"Found no VotersRecord in the log segments or the latest snapshot in "
            f"{directory}. Brokers and other observers that truncated their log while "
            f"the cluster was first starting leave the voter set out of their "
            f"snapshots. Copy the log of another node instead."
        )


class VoterNotFound(KRaftLastStandingError):
    def __init__(self, voter_id: BrokerId, voter_ids: Iterable[BrokerId]) -> None:
        listed = ", ".join(str(voter) for voter in voter_ids)
        super().__init__(
            f"Node {voter_id} is not a member of the current voter set ({listed})."
        )


class DirectoryIdMismatch(KRaftLastStandingError):
    def __init__(
        self,
        voter_id: BrokerId,
        local: uuid.UUID,
        registered: uuid.UUID | None,
    ) -> None:
        super().__init__(
            f"The directory ID of the local replica ({local}) does not match the one "
            f"registered for voter {voter_id} ({registered}). The node would not "
            f"consider itself a voter after recovery."
        )


class NothingToRecover(KRaftLastStandingError):
    def __init__(self, voter_id: BrokerId) -> None:
        super().__init__(f"Node {voter_id} is already the only voter.")


class InvalidDirectories(KRaftLastStandingError): ...


class SegmentExists(KRaftLastStandingError):
    def __init__(self, path: Path) -> None:
        super().__init__(f"Refusing to overwrite existing log segment {path}.")


class InsufficientPrivileges(KRaftLastStandingError):
    def __init__(self, directory: Path, uid: int, gid: int) -> None:
        super().__init__(
            f"Log segments in {directory} are owned by uid {uid} and gid {gid}. Run as "
            f"that user, or as root, so that created files can be given the same owner."
        )
