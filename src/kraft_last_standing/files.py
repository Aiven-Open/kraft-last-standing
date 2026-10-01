import os
import shutil
import stat

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO
from typing import Self

from .errors import SegmentChanged
from .errors import SegmentExists


@dataclass(frozen=True, slots=True, kw_only=True)
class FileAttributes:
    mode: int
    uid: int
    gid: int

    @classmethod
    def of(cls, path: Path) -> Self:
        result = path.stat()
        return cls(
            mode=stat.S_IMODE(result.st_mode),
            uid=result.st_uid,
            gid=result.st_gid,
        )


def can_apply(attributes: FileAttributes) -> bool:
    """
    Whether the current process can give files it creates the given owner. Only root
    can give away files, while an owner can change the group of a file to any group it
    is a member of.
    """
    effective_uid = os.geteuid()
    if effective_uid == 0:
        return True
    return attributes.uid == effective_uid and (
        attributes.gid == os.getegid() or attributes.gid in os.getgroups()
    )


def _apply_attributes(descriptor: int, attributes: FileAttributes) -> None:
    current = os.fstat(descriptor)
    # Changing owner requires privileges, which an operator running the tool as the
    # owner of the files doesn't have, and doesn't need.
    if (current.st_uid, current.st_gid) != (attributes.uid, attributes.gid):
        os.fchown(descriptor, attributes.uid, attributes.gid)
    os.fchmod(descriptor, attributes.mode)


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _create_new_file(
    path: Path,
    attributes: FileAttributes,
    write: Callable[[BinaryIO], object],
) -> None:
    try:
        file = path.open("xb")
    except FileExistsError:
        raise SegmentExists(path) from None
    try:
        with file:
            write(file)
            _apply_attributes(file.fileno(), attributes)
            file.flush()
            os.fsync(file.fileno())
    except BaseException:
        path.unlink()
        raise
    fsync_directory(path.parent)


def write_new_file(
    path: Path,
    content: bytes,
    attributes: FileAttributes,
) -> None:
    _create_new_file(path, attributes, lambda file: file.write(content))


def copy_new_file(
    source: Path,
    destination: Path,
    attributes: FileAttributes,
) -> None:
    # copyfile leaves the copying to the kernel where it can, but opens the destination
    # itself, overwriting any existing file, so the destination is created first.
    _create_new_file(
        destination,
        attributes,
        lambda _: shutil.copyfile(source, destination),
    )


def _write_all(descriptor: int, content: bytes) -> None:
    view = memoryview(content)
    while view:
        view = view[os.write(descriptor, view) :]


def fill_empty_file(
    path: Path,
    content: bytes,
    attributes: FileAttributes,
) -> None:
    """
    Write to an existing, empty file, such as the empty segment Kafka creates before
    appending to it, refusing a file that isn't empty. The file is truncated back to
    empty if writing fails.
    """
    descriptor = os.open(path, os.O_WRONLY)
    try:
        if os.fstat(descriptor).st_size:
            raise SegmentExists(path)
        _apply_attributes(descriptor, attributes)
        try:
            _write_all(descriptor, content)
            os.fsync(descriptor)
        except BaseException:
            os.ftruncate(descriptor, 0)
            raise
    finally:
        os.close(descriptor)


def truncate_file(path: Path, size: int, expected_size: int) -> None:
    """
    Truncate a file to ``size``, refusing it unless it's ``expected_size`` long.
    """
    descriptor = os.open(path, os.O_WRONLY)
    try:
        if os.fstat(descriptor).st_size != expected_size:
            raise SegmentChanged(path)
        os.ftruncate(descriptor, size)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
