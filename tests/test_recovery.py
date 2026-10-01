import datetime
import os
import stat

from pathlib import Path

import pytest

from kio.records.readers import read_batch
from kio.schema.types import BrokerId
from kio.static.primitive import i32
from kio.static.primitive import i64

from kraft_last_standing.errors import CopiedVotersRecordNotFound
from kraft_last_standing.errors import DirectoryIdMismatch
from kraft_last_standing.errors import InconsistentLog
from kraft_last_standing.errors import InsufficientPrivileges
from kraft_last_standing.errors import InvalidDirectories
from kraft_last_standing.errors import InvalidSnapshotName
from kraft_last_standing.errors import MetaPropertiesNotFound
from kraft_last_standing.errors import NoLogSegments
from kraft_last_standing.errors import NothingToRecover
from kraft_last_standing.errors import SegmentChanged
from kraft_last_standing.errors import VoterNotFound
from kraft_last_standing.errors import VotersRecordNotFound
from kraft_last_standing.files import FileAttributes
from kraft_last_standing.metadata_log import EffectiveLog
from kraft_last_standing.metadata_log import IncompleteBatch
from kraft_last_standing.metadata_log import LogEnd
from kraft_last_standing.metadata_log import Segment
from kraft_last_standing.metadata_log import Snapshot
from kraft_last_standing.metadata_log import list_segments
from kraft_last_standing.metadata_log import read_batches
from kraft_last_standing.metadata_log import read_log
from kraft_last_standing.metadata_log import segment_path
from kraft_last_standing.metadata_log import summarize
from kraft_last_standing.recovery import LocalReplica
from kraft_last_standing.recovery import LogCopy
from kraft_last_standing.recovery import backup_destination
from kraft_last_standing.recovery import execute_recovery
from kraft_last_standing.recovery import local_replica_from_meta_properties
from kraft_last_standing.recovery import plan_recovery

from .factories import data_batch
from .factories import directory_id
from .factories import voter
from .factories import voters_batch
from .factories import voters_record
from .factories import write_controller_log
from .factories import write_meta_properties
from .factories import write_segment
from .factories import write_snapshot

NOW = datetime.datetime(2026, 10, 1, 14, 42, 17, 123456, tzinfo=datetime.UTC)


@pytest.fixture
def log_dir(tmp_path: Path) -> Path:
    return tmp_path / "data"


@pytest.fixture
def metadata_dir(log_dir: Path) -> Path:
    write_controller_log(log_dir / "__cluster_metadata-0")
    return log_dir / "__cluster_metadata-0"


def replica(node_id: int) -> LocalReplica:
    return LocalReplica(node_id=BrokerId(node_id), directory_id=directory_id(node_id))


class TestLocalReplicaFromMetaProperties:
    def test_reads_meta_properties_in_parent_directory(
        self,
        log_dir: Path,
        metadata_dir: Path,
    ) -> None:
        write_meta_properties(log_dir, node_id=1, directory=directory_id(1))
        assert local_replica_from_meta_properties(metadata_dir) == replica(1)

    def test_resolves_relative_path(
        self,
        log_dir: Path,
        metadata_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        write_meta_properties(log_dir, node_id=1, directory=directory_id(1))
        monkeypatch.chdir(metadata_dir)
        assert local_replica_from_meta_properties(Path(".")) == replica(1)

    def test_raises_meta_properties_not_found(self, metadata_dir: Path) -> None:
        with pytest.raises(MetaPropertiesNotFound):
            local_replica_from_meta_properties(metadata_dir)


class TestPlanRecovery:
    def test_plans_demotion_to_local_voter(self, metadata_dir: Path) -> None:
        tail = segment_path(metadata_dir, 4)
        tail.chmod(0o640)
        (metadata_dir / "leader-epoch-checkpoint").touch()

        plan = plan_recovery(
            metadata_dir=metadata_dir,
            local_replica=replica(1),
            copy_from=None,
        )

        assert plan.metadata_dir == metadata_dir
        assert plan.log_copy is None
        assert plan.log_end == LogEnd(last_offset=i64(9), partition_leader_epoch=i32(3))
        assert plan.snapshot is None
        assert plan.superseded_segments == ()
        assert plan.current_voters == voters_record(0, 1, 2)
        assert plan.kept_voter == voter(1)
        assert plan.records == (voters_record(1, 0), voters_record(1))
        assert plan.removed_voter_ids == (2, 0)
        assert plan.new_segment == segment_path(metadata_dir, 10)
        assert not plan.new_segment_exists
        assert plan.incomplete_batch is None
        assert plan.leader_epoch_checkpoint is None
        assert plan.file_attributes == FileAttributes.of(tail)

    def test_plans_truncation_of_incomplete_batch(self, metadata_dir: Path) -> None:
        tail = segment_path(metadata_dir, 4)
        complete = tail.read_bytes()
        tail.write_bytes(complete + data_batch(offset=10, epoch=4)[:30])

        plan = plan_recovery(
            metadata_dir=metadata_dir,
            local_replica=replica(1),
            copy_from=None,
        )

        assert plan.log_end == LogEnd(last_offset=i64(9), partition_leader_epoch=i32(3))
        assert plan.incomplete_batch == IncompleteBatch(
            segment=Segment(base_offset=4, path=tail),
            position=len(complete),
            segment_size=len(complete) + 30,
        )
        assert plan.new_segment == segment_path(metadata_dir, 10)

    def test_plans_append_to_empty_segment_at_new_offset(
        self,
        metadata_dir: Path,
    ) -> None:
        write_segment(metadata_dir, 10)
        plan = plan_recovery(
            metadata_dir=metadata_dir,
            local_replica=replica(0),
            copy_from=None,
        )
        assert plan.new_segment == segment_path(metadata_dir, 10)
        assert plan.new_segment_exists

    def test_plans_from_snapshot_ahead_of_segments(self, metadata_dir: Path) -> None:
        snapshot = write_snapshot(metadata_dir, 28, 5, voters_record(0, 1, 2, 3))
        checkpoint = metadata_dir / "leader-epoch-checkpoint"
        checkpoint.touch()

        plan = plan_recovery(
            metadata_dir=metadata_dir,
            local_replica=replica(1),
            copy_from=None,
        )

        assert plan.log_end == LogEnd(
            last_offset=i64(27), partition_leader_epoch=i32(5)
        )
        assert plan.snapshot == Snapshot(
            end_offset=i64(28),
            epoch=i32(5),
            path=snapshot,
        )
        assert plan.superseded_segments == list_segments(metadata_dir)
        assert plan.current_voters == voters_record(0, 1, 2, 3)
        assert plan.new_segment == segment_path(metadata_dir, 28)
        assert not plan.new_segment_exists
        assert plan.leader_epoch_checkpoint == checkpoint

    def test_skips_directory_id_verification_when_not_given(
        self,
        metadata_dir: Path,
    ) -> None:
        plan = plan_recovery(
            metadata_dir=metadata_dir,
            local_replica=LocalReplica(node_id=BrokerId(0), directory_id=None),
            copy_from=None,
        )
        assert plan.kept_voter == voter(0)

    def test_raises_directory_id_mismatch(self, metadata_dir: Path) -> None:
        with pytest.raises(DirectoryIdMismatch):
            plan_recovery(
                metadata_dir=metadata_dir,
                local_replica=LocalReplica(
                    node_id=BrokerId(0),
                    directory_id=directory_id(1),
                ),
                copy_from=None,
            )

    def test_raises_voter_not_found(self, metadata_dir: Path) -> None:
        with pytest.raises(VoterNotFound):
            plan_recovery(
                metadata_dir=metadata_dir,
                local_replica=replica(3),
                copy_from=None,
            )

    def test_raises_nothing_to_recover_for_only_voter(self, tmp_path: Path) -> None:
        write_segment(tmp_path, 0, voters_batch(voters_record(0), offset=0))
        with pytest.raises(NothingToRecover):
            plan_recovery(
                metadata_dir=tmp_path,
                local_replica=replica(0),
                copy_from=None,
            )

    def test_raises_no_log_segments_for_empty_log(self, tmp_path: Path) -> None:
        write_segment(tmp_path, 0)
        with pytest.raises(NoLogSegments):
            plan_recovery(
                metadata_dir=tmp_path,
                local_replica=replica(0),
                copy_from=None,
            )

    def test_raises_voters_record_not_found(self, tmp_path: Path) -> None:
        write_segment(tmp_path, 0, data_batch(offset=0))
        with pytest.raises(VotersRecordNotFound, match="snapshot"):
            plan_recovery(
                metadata_dir=tmp_path,
                local_replica=replica(0),
                copy_from=None,
            )

    def test_raises_inconsistent_log(self, metadata_dir: Path) -> None:
        write_segment(metadata_dir, 15)
        with pytest.raises(InconsistentLog):
            plan_recovery(
                metadata_dir=metadata_dir,
                local_replica=replica(0),
                copy_from=None,
            )

    def test_raises_insufficient_privileges_for_foreign_owner(
        self,
        metadata_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        other_user = os.geteuid() + 1
        monkeypatch.setattr(os, "geteuid", lambda: other_user)
        with pytest.raises(InsufficientPrivileges):
            plan_recovery(
                metadata_dir=metadata_dir,
                local_replica=replica(0),
                copy_from=None,
            )


class TestPlanRecoveryCopyingLog:
    def test_plans_from_copied_segments(
        self,
        tmp_path: Path,
        metadata_dir: Path,
    ) -> None:
        source = tmp_path / "broker" / "__cluster_metadata-0"
        write_controller_log(source)
        write_segment(source, 10, data_batch(offset=10, record_count=5, epoch=4))
        target_tail = segment_path(metadata_dir, 4)
        target_tail.chmod(0o600)
        target_snapshot = write_snapshot(metadata_dir, 5, 3, None)
        checkpoint = metadata_dir / "leader-epoch-checkpoint"
        checkpoint.touch()

        plan = plan_recovery(
            metadata_dir=metadata_dir,
            local_replica=replica(0),
            copy_from=source,
        )

        assert plan.log_copy == LogCopy(
            source_dir=source,
            segments=list_segments(source),
            snapshot=None,
            replaced=(segment_path(metadata_dir, 0), target_tail, target_snapshot),
        )
        assert plan.log_end == LogEnd(
            last_offset=i64(14), partition_leader_epoch=i32(4)
        )
        assert plan.snapshot is None
        assert plan.new_segment == segment_path(metadata_dir, 15)
        assert plan.leader_epoch_checkpoint == checkpoint
        assert plan.file_attributes == FileAttributes.of(target_tail)

    def test_plans_from_copied_snapshot(
        self,
        tmp_path: Path,
        metadata_dir: Path,
    ) -> None:
        source = tmp_path / "broker" / "__cluster_metadata-0"
        write_segment(source, 10, data_batch(offset=10, record_count=5, epoch=4))
        older = write_snapshot(source, 10, 4, voters_record(0, 1))
        latest = write_snapshot(source, 12, 4, voters_record(0, 1, 2))

        plan = plan_recovery(
            metadata_dir=metadata_dir,
            local_replica=replica(0),
            copy_from=source,
        )

        assert plan.log_copy is not None
        assert plan.log_copy.snapshot == Snapshot(
            end_offset=i64(12), epoch=i32(4), path=latest
        )
        assert older.exists()
        assert plan.snapshot == plan.log_copy.snapshot
        assert plan.current_voters == voters_record(0, 1, 2)
        assert plan.superseded_segments == ()
        assert plan.leader_epoch_checkpoint is None

    def test_raises_inconsistent_log_for_copied_segments_without_snapshot(
        self,
        tmp_path: Path,
        metadata_dir: Path,
    ) -> None:
        source = tmp_path / "broker" / "__cluster_metadata-0"
        write_segment(source, 10, data_batch(offset=10, record_count=5, epoch=4))
        write_snapshot(source, 5, 4, voters_record(0, 1))
        with pytest.raises(InconsistentLog, match="broker"):
            plan_recovery(
                metadata_dir=metadata_dir,
                local_replica=replica(0),
                copy_from=source,
            )

    def test_raises_copied_voters_record_not_found(
        self,
        tmp_path: Path,
        metadata_dir: Path,
    ) -> None:
        source = tmp_path / "broker" / "__cluster_metadata-0"
        write_segment(source, 10, data_batch(offset=10, record_count=5, epoch=4))
        write_snapshot(source, 10, 4, None)
        with pytest.raises(CopiedVotersRecordNotFound, match="another node"):
            plan_recovery(
                metadata_dir=metadata_dir,
                local_replica=replica(0),
                copy_from=source,
            )

    def test_raises_invalid_snapshot_name_in_target(
        self,
        tmp_path: Path,
        metadata_dir: Path,
    ) -> None:
        source = tmp_path / "broker" / "__cluster_metadata-0"
        write_controller_log(source)
        (metadata_dir / "foo.checkpoint").touch()
        with pytest.raises(InvalidSnapshotName):
            plan_recovery(
                metadata_dir=metadata_dir,
                local_replica=replica(0),
                copy_from=source,
            )

    def test_uses_directory_owner_when_target_has_no_segments(
        self,
        tmp_path: Path,
    ) -> None:
        source = tmp_path / "broker" / "__cluster_metadata-0"
        write_controller_log(source)
        target = tmp_path / "controller" / "__cluster_metadata-0"
        target.mkdir(parents=True)

        plan = plan_recovery(
            metadata_dir=target,
            local_replica=replica(0),
            copy_from=source,
        )

        directory = target.stat()
        assert plan.file_attributes == FileAttributes(
            mode=0o644,
            uid=directory.st_uid,
            gid=directory.st_gid,
        )

    def test_raises_no_log_segments_naming_source(
        self,
        tmp_path: Path,
        metadata_dir: Path,
    ) -> None:
        source = tmp_path / "broker" / "__cluster_metadata-0"
        source.mkdir(parents=True)
        with pytest.raises(NoLogSegments, match="broker"):
            plan_recovery(
                metadata_dir=metadata_dir,
                local_replica=replica(0),
                copy_from=source,
            )

    def test_raises_invalid_directories_for_copy_from_itself(
        self,
        metadata_dir: Path,
    ) -> None:
        with pytest.raises(InvalidDirectories, match="metadata directory itself"):
            plan_recovery(
                metadata_dir=metadata_dir,
                local_replica=replica(0),
                copy_from=metadata_dir / ".." / metadata_dir.name,
            )

    def test_raises_invalid_directories_for_missing_source(
        self,
        tmp_path: Path,
        metadata_dir: Path,
    ) -> None:
        with pytest.raises(InvalidDirectories, match="is not a directory"):
            plan_recovery(
                metadata_dir=metadata_dir,
                local_replica=replica(0),
                copy_from=tmp_path / "missing",
            )


class TestBackupDestination:
    def test_returns_timestamped_path(self, tmp_path: Path, metadata_dir: Path) -> None:
        assert backup_destination(metadata_dir, tmp_path / "backup", NOW) == (
            tmp_path / "backup" / "__cluster_metadata-0.20261001T144217Z"
        )

    def test_converts_timestamp_to_utc(
        self, tmp_path: Path, metadata_dir: Path
    ) -> None:
        now = NOW.astimezone(datetime.timezone(datetime.timedelta(hours=2)))
        destination = backup_destination(metadata_dir, tmp_path, now)
        assert destination.name == "__cluster_metadata-0.20261001T144217Z"

    def test_raises_invalid_directories_for_backup_inside_metadata_dir(
        self,
        metadata_dir: Path,
    ) -> None:
        with pytest.raises(InvalidDirectories, match="must not be inside"):
            backup_destination(metadata_dir, metadata_dir / "backup", NOW)

    def test_raises_invalid_directories_for_existing_destination(
        self,
        tmp_path: Path,
        metadata_dir: Path,
    ) -> None:
        backup_destination(metadata_dir, tmp_path, NOW).mkdir()
        with pytest.raises(InvalidDirectories, match="exists"):
            backup_destination(metadata_dir, tmp_path, NOW)


def _snapshot(directory: Path) -> dict[str, bytes]:
    return {path.name: path.read_bytes() for path in directory.iterdir()}


class TestExecuteRecovery:
    def test_appends_voters_records_in_new_segment(
        self,
        tmp_path: Path,
        metadata_dir: Path,
    ) -> None:
        segment_path(metadata_dir, 0).chmod(0o640)
        segment_path(metadata_dir, 4).chmod(0o640)
        (metadata_dir / "quorum-state").write_text("{}")
        (metadata_dir / "leader-epoch-checkpoint").write_text("0\n1\n3 0\n")
        original = _snapshot(metadata_dir)
        plan = plan_recovery(
            metadata_dir=metadata_dir,
            local_replica=replica(1),
            copy_from=None,
        )
        backup = tmp_path / "backups" / "nested" / "backup"

        execute_recovery(plan, backup, NOW)

        assert stat.S_IMODE(backup.stat().st_mode) == 0o700
        assert [path.name for path in backup.iterdir()] == ["__cluster_metadata-0"]
        assert _snapshot(backup / "__cluster_metadata-0") == original
        assert _snapshot(metadata_dir) == {
            **original,
            "00000000000000000010.log": plan.new_segment.read_bytes(),
        }
        assert stat.S_IMODE(plan.new_segment.stat().st_mode) == 0o640

        new_segment = plan.new_segment.read_bytes()
        first, first_size = read_batch(new_segment, 0)
        second, second_size = read_batch(new_segment, first_size)
        assert first_size + second_size == len(new_segment)
        assert (first.base_offset, second.base_offset) == (10, 11)
        assert {first.partition_leader_epoch, second.partition_leader_epoch} == {3}
        expected_timestamp = NOW.replace(microsecond=0)
        assert first.records[0].timestamp == expected_timestamp

        summary = summarize(read_batches(list_segments(metadata_dir)))
        assert summary is not None
        assert summary.end == LogEnd(last_offset=i64(11), partition_leader_epoch=i32(3))
        assert summary.voters == voters_record(1)

    def test_writes_into_empty_segment_at_snapshot_end(
        self,
        tmp_path: Path,
    ) -> None:
        metadata_dir = tmp_path / "__cluster_metadata-0"
        empty = write_segment(metadata_dir, 28)
        empty.chmod(0o640)
        write_snapshot(metadata_dir, 28, 5, voters_record(0, 1, 2))
        plan = plan_recovery(
            metadata_dir=metadata_dir,
            local_replica=replica(1),
            copy_from=None,
        )
        original = _snapshot(metadata_dir)

        execute_recovery(plan, tmp_path / "backup", NOW)

        assert _snapshot(tmp_path / "backup" / "__cluster_metadata-0") == original
        assert sorted(path.name for path in metadata_dir.iterdir()) == sorted(original)
        assert stat.S_IMODE(empty.stat().st_mode) == 0o640
        assert read_log(metadata_dir) == EffectiveLog(
            end=LogEnd(last_offset=i64(29), partition_leader_epoch=i32(5)),
            voters=voters_record(1),
            snapshot=plan.snapshot,
            superseded=(),
            append_segment=None,
            incomplete_batch=None,
        )

    def test_deletes_segments_behind_snapshot(
        self,
        tmp_path: Path,
        metadata_dir: Path,
    ) -> None:
        snapshot = write_snapshot(metadata_dir, 28, 5, voters_record(0, 1, 2))
        segment_path(metadata_dir, 4).chmod(0o640)
        (metadata_dir / "leader-epoch-checkpoint").write_text("0\n1\n3 0\n")
        plan = plan_recovery(
            metadata_dir=metadata_dir,
            local_replica=replica(1),
            copy_from=None,
        )
        original = _snapshot(metadata_dir)

        execute_recovery(plan, tmp_path / "backup", NOW)

        assert _snapshot(tmp_path / "backup" / "__cluster_metadata-0") == original
        assert sorted(path.name for path in metadata_dir.iterdir()) == [
            "00000000000000000028-0000000005.checkpoint",
            "00000000000000000028.log",
        ]
        assert stat.S_IMODE(plan.new_segment.stat().st_mode) == 0o640
        assert read_log(metadata_dir) == EffectiveLog(
            end=LogEnd(last_offset=i64(29), partition_leader_epoch=i32(5)),
            voters=voters_record(1),
            snapshot=Snapshot(end_offset=i64(28), epoch=i32(5), path=snapshot),
            superseded=(),
            append_segment=None,
            incomplete_batch=None,
        )

    def test_replaces_snapshots_and_skips_superseded_copies(
        self,
        tmp_path: Path,
        metadata_dir: Path,
    ) -> None:
        source = tmp_path / "broker" / "__cluster_metadata-0"
        write_segment(
            source,
            0,
            voters_batch(voters_record(0, 1), offset=0, epoch=3),
            data_batch(offset=1, record_count=19, epoch=3),
        )
        write_snapshot(source, 10, 3, voters_record(0, 1))
        source_snapshot = write_snapshot(source, 28, 5, voters_record(0, 1, 2))
        write_snapshot(metadata_dir, 5, 3, None)
        (metadata_dir / "00000000000000000004.index").write_bytes(b"index")
        plan = plan_recovery(
            metadata_dir=metadata_dir,
            local_replica=replica(1),
            copy_from=source,
        )
        original = _snapshot(metadata_dir)

        execute_recovery(plan, tmp_path / "backup", NOW)

        assert _snapshot(tmp_path / "backup" / "__cluster_metadata-0") == original
        assert sorted(path.name for path in metadata_dir.iterdir()) == [
            "00000000000000000004.index",
            "00000000000000000028-0000000005.checkpoint",
            "00000000000000000028.log",
        ]
        copied = metadata_dir / source_snapshot.name
        assert copied.read_bytes() == source_snapshot.read_bytes()
        assert stat.S_IMODE(copied.stat().st_mode) == 0o400
        assert read_log(metadata_dir) == EffectiveLog(
            end=LogEnd(last_offset=i64(29), partition_leader_epoch=i32(5)),
            voters=voters_record(1),
            snapshot=Snapshot(end_offset=i64(28), epoch=i32(5), path=copied),
            superseded=(),
            append_segment=None,
            incomplete_batch=None,
        )

    def test_replaces_segments_when_copying_log(
        self,
        tmp_path: Path,
        metadata_dir: Path,
    ) -> None:
        source = tmp_path / "broker" / "__cluster_metadata-0"
        write_segment(
            source,
            0,
            voters_batch(voters_record(0, 1, 2), offset=0),
            data_batch(offset=1, record_count=19, epoch=5),
        )
        source_segment = segment_path(source, 0)
        source_segment.chmod(0o600)
        segment_path(metadata_dir, 4).chmod(0o640)
        (metadata_dir / "00000000000000000004.index").write_bytes(b"index")
        (metadata_dir / "leader-epoch-checkpoint").write_text("0\n1\n3 0\n")
        plan = plan_recovery(
            metadata_dir=metadata_dir,
            local_replica=replica(2),
            copy_from=source,
        )
        original = _snapshot(metadata_dir)

        execute_recovery(plan, tmp_path / "backup", NOW)

        assert _snapshot(tmp_path / "backup" / "__cluster_metadata-0") == original
        assert sorted(path.name for path in metadata_dir.iterdir()) == [
            "00000000000000000000.log",
            "00000000000000000004.index",
            "00000000000000000020.log",
        ]
        copied = segment_path(metadata_dir, 0)
        assert copied.read_bytes() == source_segment.read_bytes()
        assert stat.S_IMODE(copied.stat().st_mode) == 0o640
        assert stat.S_IMODE(plan.new_segment.stat().st_mode) == 0o640

        summary = summarize(read_batches(list_segments(metadata_dir)))
        assert summary is not None
        assert summary.end == LogEnd(last_offset=i64(21), partition_leader_epoch=i32(5))
        assert summary.voters == voters_record(2)

    def test_truncates_incomplete_batch_before_appending(
        self,
        tmp_path: Path,
        metadata_dir: Path,
    ) -> None:
        tail = segment_path(metadata_dir, 4)
        tail.chmod(0o640)
        complete = tail.read_bytes()
        tail.write_bytes(complete + data_batch(offset=10, epoch=4)[:30])
        original = _snapshot(metadata_dir)
        plan = plan_recovery(
            metadata_dir=metadata_dir,
            local_replica=replica(1),
            copy_from=None,
        )

        execute_recovery(plan, tmp_path / "backup", NOW)

        assert _snapshot(tmp_path / "backup" / "__cluster_metadata-0") == original
        assert tail.read_bytes() == complete
        assert stat.S_IMODE(tail.stat().st_mode) == 0o640
        summary = summarize(read_batches(list_segments(metadata_dir)))
        assert summary is not None
        assert summary.end == LogEnd(last_offset=i64(11), partition_leader_epoch=i32(3))
        assert summary.voters == voters_record(1)

    def test_writes_into_segment_holding_only_incomplete_batch(
        self,
        tmp_path: Path,
        metadata_dir: Path,
    ) -> None:
        incomplete = write_segment(
            metadata_dir,
            10,
            data_batch(offset=10, epoch=4)[:30],
        )
        plan = plan_recovery(
            metadata_dir=metadata_dir,
            local_replica=replica(1),
            copy_from=None,
        )

        execute_recovery(plan, tmp_path / "backup", NOW)

        assert plan.new_segment == incomplete
        assert read_log(metadata_dir) == EffectiveLog(
            end=LogEnd(last_offset=i64(11), partition_leader_epoch=i32(3)),
            voters=voters_record(1),
            snapshot=None,
            superseded=(),
            append_segment=None,
            incomplete_batch=None,
        )

    def test_truncates_incomplete_batch_at_snapshot_end(
        self,
        tmp_path: Path,
        metadata_dir: Path,
    ) -> None:
        incomplete = write_segment(
            metadata_dir,
            28,
            data_batch(offset=28, epoch=5)[:30],
        )
        snapshot = write_snapshot(metadata_dir, 28, 5, voters_record(0, 1, 2))
        plan = plan_recovery(
            metadata_dir=metadata_dir,
            local_replica=replica(1),
            copy_from=None,
        )

        execute_recovery(plan, tmp_path / "backup", NOW)

        assert plan.new_segment == incomplete
        assert sorted(path.name for path in metadata_dir.iterdir()) == [
            "00000000000000000028-0000000005.checkpoint",
            "00000000000000000028.log",
        ]
        assert read_log(metadata_dir) == EffectiveLog(
            end=LogEnd(last_offset=i64(29), partition_leader_epoch=i32(5)),
            voters=voters_record(1),
            snapshot=Snapshot(end_offset=i64(28), epoch=i32(5), path=snapshot),
            superseded=(),
            append_segment=None,
            incomplete_batch=None,
        )

    def test_raises_segment_changed_before_modifying_anything(
        self,
        tmp_path: Path,
        metadata_dir: Path,
    ) -> None:
        incomplete = write_segment(
            metadata_dir,
            28,
            data_batch(offset=28, epoch=5)[:30],
        )
        write_snapshot(metadata_dir, 28, 5, voters_record(0, 1, 2))
        (metadata_dir / "leader-epoch-checkpoint").touch()
        plan = plan_recovery(
            metadata_dir=metadata_dir,
            local_replica=replica(1),
            copy_from=None,
        )
        incomplete.write_bytes(incomplete.read_bytes() + b"more")
        changed = _snapshot(metadata_dir)

        with pytest.raises(SegmentChanged):
            execute_recovery(plan, tmp_path / "backup", NOW)

        assert _snapshot(metadata_dir) == changed

    def test_leaves_incomplete_batch_out_of_copy(
        self,
        tmp_path: Path,
        metadata_dir: Path,
    ) -> None:
        source = tmp_path / "broker" / "__cluster_metadata-0"
        write_controller_log(source)
        source_tail = segment_path(source, 4)
        complete = source_tail.read_bytes()
        source_tail.write_bytes(complete + data_batch(offset=10, epoch=4)[:30])
        original_source = _snapshot(source)
        plan = plan_recovery(
            metadata_dir=metadata_dir,
            local_replica=replica(1),
            copy_from=source,
        )

        execute_recovery(plan, tmp_path / "backup", NOW)

        assert _snapshot(source) == original_source
        assert segment_path(metadata_dir, 4).read_bytes() == complete
        summary = summarize(read_batches(list_segments(metadata_dir)))
        assert summary is not None
        assert summary.end == LogEnd(last_offset=i64(11), partition_leader_epoch=i32(3))
