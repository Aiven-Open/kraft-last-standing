import os
import stat

from pathlib import Path

import pytest

from kraft_last_standing.errors import SegmentChanged
from kraft_last_standing.errors import SegmentExists
from kraft_last_standing.files import FileAttributes
from kraft_last_standing.files import can_apply
from kraft_last_standing.files import copy_new_file
from kraft_last_standing.files import fill_empty_file
from kraft_last_standing.files import truncate_file
from kraft_last_standing.files import write_new_file

running_as_root = pytest.mark.skipif(
    os.geteuid() == 0,
    reason="root can give files to any owner",
)


def own(mode: int) -> FileAttributes:
    return FileAttributes(mode=mode, uid=os.geteuid(), gid=os.getegid())


def test_file_attributes_of_returns_mode_and_owner(tmp_path: Path) -> None:
    path = tmp_path / "file"
    path.touch()
    path.chmod(0o640)
    assert FileAttributes.of(path) == own(0o640)


class TestCanApply:
    def test_returns_true_for_own_user_and_group(self) -> None:
        assert can_apply(own(0o644))

    def test_returns_true_for_supplementary_group(self) -> None:
        for group in os.getgroups():
            attributes = FileAttributes(mode=0o644, uid=os.geteuid(), gid=group)
            assert can_apply(attributes)

    @running_as_root
    def test_returns_false_for_other_user(self) -> None:
        attributes = FileAttributes(mode=0o644, uid=os.geteuid() + 1, gid=os.getegid())
        assert not can_apply(attributes)

    @running_as_root
    def test_returns_false_for_foreign_group(self) -> None:
        foreign = max(os.getegid(), *os.getgroups()) + 1
        attributes = FileAttributes(mode=0o644, uid=os.geteuid(), gid=foreign)
        assert not can_apply(attributes)

    def test_returns_true_for_root(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(os, "geteuid", lambda: 0)
        assert can_apply(FileAttributes(mode=0o644, uid=1234, gid=1234))


class TestWriteNewFile:
    def test_writes_content_with_attributes(self, tmp_path: Path) -> None:
        path = tmp_path / "file"
        write_new_file(path, b"content", own(0o600))
        assert path.read_bytes() == b"content"
        assert stat.S_IMODE(path.stat().st_mode) == 0o600

    def test_raises_segment_exists_for_existing_file(self, tmp_path: Path) -> None:
        path = tmp_path / "file"
        path.write_bytes(b"existing")
        with pytest.raises(SegmentExists):
            write_new_file(path, b"content", own(0o600))
        assert path.read_bytes() == b"existing"

    @running_as_root
    def test_removes_file_when_attributes_cannot_be_applied(
        self,
        tmp_path: Path,
    ) -> None:
        path = tmp_path / "file"
        attributes = FileAttributes(mode=0o644, uid=os.geteuid() + 1, gid=os.getegid())
        with pytest.raises(PermissionError):
            write_new_file(path, b"content", attributes)
        assert not path.exists()


class TestCopyNewFile:
    def test_copies_content_with_attributes(self, tmp_path: Path) -> None:
        source = tmp_path / "source"
        source.write_bytes(b"content")
        destination = tmp_path / "destination"

        copy_new_file(source, destination, own(0o400))

        assert destination.read_bytes() == b"content"
        assert stat.S_IMODE(destination.stat().st_mode) == 0o400

    def test_raises_segment_exists_for_existing_file(self, tmp_path: Path) -> None:
        source = tmp_path / "source"
        source.write_bytes(b"content")
        destination = tmp_path / "destination"
        destination.write_bytes(b"existing")
        with pytest.raises(SegmentExists):
            copy_new_file(source, destination, own(0o600))
        assert destination.read_bytes() == b"existing"

    def test_removes_file_when_copying_fails(self, tmp_path: Path) -> None:
        destination = tmp_path / "destination"
        with pytest.raises(FileNotFoundError):
            copy_new_file(tmp_path / "missing", destination, own(0o600))
        assert not destination.exists()


class TestFillEmptyFile:
    def test_writes_content_with_attributes(self, tmp_path: Path) -> None:
        path = tmp_path / "file"
        path.touch(mode=0o644)
        fill_empty_file(path, b"content", own(0o600))
        assert path.read_bytes() == b"content"
        assert stat.S_IMODE(path.stat().st_mode) == 0o600

    def test_writes_all_content_across_partial_writes(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        write = os.write
        monkeypatch.setattr(
            os,
            "write",
            lambda descriptor, data: write(descriptor, bytes(data[:3])),
        )
        path = tmp_path / "file"
        path.touch()
        fill_empty_file(path, b"content", own(0o600))
        assert path.read_bytes() == b"content"

    def test_raises_segment_exists_for_non_empty_file(self, tmp_path: Path) -> None:
        path = tmp_path / "file"
        path.write_bytes(b"existing")
        with pytest.raises(SegmentExists):
            fill_empty_file(path, b"content", own(0o600))
        assert path.read_bytes() == b"existing"

    def test_truncates_file_when_writing_fails(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def fail(descriptor: int) -> None:
            raise OSError("fsync failed")

        monkeypatch.setattr(os, "fsync", fail)
        path = tmp_path / "file"
        path.touch()
        with pytest.raises(OSError, match="fsync failed"):
            fill_empty_file(path, b"content", own(0o600))
        assert path.read_bytes() == b""

    @running_as_root
    def test_leaves_file_empty_when_attributes_cannot_be_applied(
        self,
        tmp_path: Path,
    ) -> None:
        path = tmp_path / "file"
        path.touch()
        attributes = FileAttributes(mode=0o644, uid=os.geteuid() + 1, gid=os.getegid())
        with pytest.raises(PermissionError):
            fill_empty_file(path, b"content", attributes)
        assert path.read_bytes() == b""

    def test_raises_file_not_found_for_missing_file(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            fill_empty_file(tmp_path / "missing", b"content", own(0o600))
        assert not (tmp_path / "missing").exists()


class TestTruncateFile:
    def test_truncates_file(self, tmp_path: Path) -> None:
        path = tmp_path / "file"
        path.write_bytes(b"content")
        path.chmod(0o640)
        truncate_file(path, 4, expected_size=7)
        assert path.read_bytes() == b"cont"
        assert stat.S_IMODE(path.stat().st_mode) == 0o640

    def test_raises_segment_changed_for_unexpected_size(self, tmp_path: Path) -> None:
        path = tmp_path / "file"
        path.write_bytes(b"content")
        with pytest.raises(SegmentChanged):
            truncate_file(path, 4, expected_size=6)
        assert path.read_bytes() == b"content"
