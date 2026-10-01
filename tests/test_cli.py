import importlib
import json
import logging
import runpy
import sys

from pathlib import Path

import pytest

from kio.schema.types import BrokerId

from kraft_last_standing import __version__
from kraft_last_standing.cli import ProbeCommand
from kraft_last_standing.cli import RecoverCommand
from kraft_last_standing.cli import format_plan
from kraft_last_standing.cli import main
from kraft_last_standing.cli import parse_command
from kraft_last_standing.metadata_log import list_segments
from kraft_last_standing.metadata_log import read_batches
from kraft_last_standing.metadata_log import segment_path
from kraft_last_standing.metadata_log import summarize
from kraft_last_standing.recovery import LocalReplica
from kraft_last_standing.recovery import plan_recovery

from .factories import corrupt_batch
from .factories import data_batch
from .factories import directory_id
from .factories import voters_record
from .factories import write_controller_log
from .factories import write_meta_properties
from .factories import write_segment
from .factories import write_snapshot


@pytest.fixture
def metadata_dir(tmp_path: Path) -> Path:
    log_dir = tmp_path / "controller"
    write_controller_log(log_dir / "__cluster_metadata-0")
    write_meta_properties(log_dir, node_id=0, directory=directory_id(0))
    return log_dir / "__cluster_metadata-0"


def _snapshot(directory: Path) -> dict[str, bytes]:
    return {path.name: path.read_bytes() for path in directory.iterdir()}


class TestParseCommand:
    def test_parses_probe(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        assert parse_command(["probe", "a", "/b", "--json"]) == ProbeCommand(
            metadata_dirs=(tmp_path / "a", Path("/b")),
            json=True,
        )

    def test_parses_recover(self) -> None:
        assert parse_command(
            [
                "recover",
                "--metadata-dir=/data/__cluster_metadata-0",
                "--backup-dir=/backup",
                "--node-id=3",
                "--copy-log-from=/broker/__cluster_metadata-0",
                "--yes",
            ]
        ) == RecoverCommand(
            metadata_dir=Path("/data/__cluster_metadata-0"),
            backup_dir=Path("/backup"),
            node_id=BrokerId(3),
            copy_log_from=Path("/broker/__cluster_metadata-0"),
            yes=True,
        )

    def test_parses_recover_defaults(self) -> None:
        assert parse_command(
            ["recover", "--metadata-dir=/data", "--backup-dir=/backup"]
        ) == RecoverCommand(
            metadata_dir=Path("/data"),
            backup_dir=Path("/backup"),
            node_id=None,
            copy_log_from=None,
            yes=False,
        )

    @pytest.mark.parametrize("node_id", ["one", "2147483648"])
    def test_rejects_invalid_node_id(
        self,
        capsys: pytest.CaptureFixture[str],
        node_id: str,
    ) -> None:
        with pytest.raises(SystemExit) as exc_info:
            parse_command(
                [
                    "recover",
                    "--metadata-dir=/a",
                    "--backup-dir=/b",
                    f"--node-id={node_id}",
                ]
            )
        assert exc_info.value.code == 2
        assert f"invalid node ID: {node_id!r}" in capsys.readouterr().err

    def test_requires_command(self, capsys: pytest.CaptureFixture[str]) -> None:
        with pytest.raises(SystemExit) as exc_info:
            parse_command([])
        assert exc_info.value.code == 2
        assert "COMMAND" in capsys.readouterr().err

    def test_prints_version(self, capsys: pytest.CaptureFixture[str]) -> None:
        with pytest.raises(SystemExit) as exc_info:
            parse_command(["--version"])
        assert exc_info.value.code == 0
        assert capsys.readouterr().out == f"kraft-last-standing {__version__}\n"


def test_module_runs_main(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "argv", ["kraft-last-standing", "--version"])
    with pytest.raises(SystemExit) as exc_info:
        runpy.run_module("kraft_last_standing", run_name="__main__")
    assert exc_info.value.code == 0
    assert __version__ in capsys.readouterr().out


def test_importing_module_does_not_run_main(
    capsys: pytest.CaptureFixture[str],
) -> None:
    importlib.import_module("kraft_last_standing.__main__")
    assert capsys.readouterr() == ("", "")


class TestProbe:
    @pytest.fixture
    def empty_dir(self, tmp_path: Path) -> Path:
        directory = tmp_path / "broker" / "__cluster_metadata-0"
        directory.mkdir(parents=True)
        return directory

    def test_prints_results_most_progressed_first(
        self,
        capsys: pytest.CaptureFixture[str],
        metadata_dir: Path,
        empty_dir: Path,
    ) -> None:
        write_snapshot(metadata_dir, 0, 0, voters_record(0))
        assert main(["probe", str(empty_dir), str(metadata_dir)]) == 0
        assert capsys.readouterr().out == (
            f"{metadata_dir}\n"
            "  node ID:      0\n"
            "  last offset:  9\n"
            "  leader epoch: 3\n"
            "  snapshot:     00000000000000000000-0000000000.checkpoint\n"
            "  voters:       0, 1, 2\n"
            "\n"
            f"{empty_dir}\n"
            "  node ID:      unknown, found no meta.properties\n"
            "  last offset:  none, found no record batches\n"
            "  leader epoch: none\n"
            "  snapshot:     none\n"
            "  voters:       unknown, found no VotersRecord in the log or snapshot\n"
        )

    def test_prints_json(
        self,
        capsys: pytest.CaptureFixture[str],
        metadata_dir: Path,
        empty_dir: Path,
    ) -> None:
        write_snapshot(metadata_dir, 0, 0, voters_record(0))
        assert main(["probe", "--json", str(empty_dir), str(metadata_dir)]) == 0
        assert json.loads(capsys.readouterr().out) == [
            {
                "directory": str(metadata_dir),
                "node_id": 0,
                "last_offset": 9,
                "leader_epoch": 3,
                "snapshot": "00000000000000000000-0000000000.checkpoint",
                "voter_ids": [0, 1, 2],
            },
            {
                "directory": str(empty_dir),
                "node_id": None,
                "last_offset": None,
                "leader_epoch": None,
                "snapshot": None,
                "voter_ids": None,
            },
        ]

    def test_reports_error(
        self,
        capsys: pytest.CaptureFixture[str],
        tmp_path: Path,
    ) -> None:
        write_segment(tmp_path, 0, corrupt_batch(offset=0))
        assert main(["probe", str(tmp_path)]) == 1
        assert capsys.readouterr().err.startswith("error: Failed reading record batch")


def test_format_plan_describes_recovery(tmp_path: Path, metadata_dir: Path) -> None:
    source = tmp_path / "broker" / "__cluster_metadata-0"
    write_controller_log(source)
    plan = plan_recovery(
        metadata_dir=metadata_dir,
        local_replica=LocalReplica(node_id=BrokerId(1), directory_id=None),
        copy_from=source,
    )

    assert format_plan(plan, tmp_path / "backup") == (
        "Recovery plan\n"
        f"  metadata directory: {metadata_dir}\n"
        f"  replace log with:   a copy of the one in {source}\n"
        f"  backup destination: {tmp_path / 'backup'}\n"
        "  last offset:        9\n"
        "  leader epoch:       3\n"
        "  snapshot:           none\n"
        "  current voters:     0, 1, 2\n"
        "  voter to keep:      1 (directory ID AAAAAAAAAAAAAAAAAAAQAQ)\n"
        "  voters to remove:   2, 0, in this order\n"
        f"  new log segment:    {metadata_dir / '00000000000000000010.log'}\n"
        "  offsets to append:  10 to 11"
    )


def test_format_plan_describes_skipped_copies(
    tmp_path: Path, metadata_dir: Path
) -> None:
    source = tmp_path / "broker" / "__cluster_metadata-0"
    write_controller_log(source)
    write_snapshot(source, 28, 5, voters_record(0, 1))
    plan = plan_recovery(
        metadata_dir=metadata_dir,
        local_replica=LocalReplica(node_id=BrokerId(1), directory_id=None),
        copy_from=source,
    )

    assert (
        "  segments to skip:   00000000000000000000.log, 00000000000000000004.log, "
        "which the snapshot is ahead of\n"
    ) in format_plan(plan, tmp_path / "backup")


@pytest.mark.parametrize(
    ("copy", "action"),
    [
        pytest.param(False, "truncated", id="own-log"),
        pytest.param(True, "left out of the copy", id="copy"),
    ],
)
def test_format_plan_describes_incomplete_batch(
    tmp_path: Path,
    metadata_dir: Path,
    copy: bool,
    action: str,
) -> None:
    source = tmp_path / "broker" / "__cluster_metadata-0"
    write_controller_log(source)
    tail = segment_path(source if copy else metadata_dir, 4)
    complete = tail.read_bytes()
    tail.write_bytes(complete + data_batch(offset=10)[:30])
    plan = plan_recovery(
        metadata_dir=metadata_dir,
        local_replica=LocalReplica(node_id=BrokerId(1), directory_id=None),
        copy_from=source if copy else None,
    )

    assert (
        f"  incomplete batch:   at byte position {len(complete)} of "
        f"00000000000000000004.log, which is {action}\n"
    ) in format_plan(plan, tmp_path / "backup")


def test_format_plan_describes_starting_over_at_snapshot(
    tmp_path: Path,
    metadata_dir: Path,
) -> None:
    write_segment(metadata_dir, 28)
    write_snapshot(metadata_dir, 28, 5, voters_record(0, 1))
    (metadata_dir / "leader-epoch-checkpoint").touch()
    plan = plan_recovery(
        metadata_dir=metadata_dir,
        local_replica=LocalReplica(node_id=BrokerId(0), directory_id=None),
        copy_from=None,
    )

    assert format_plan(plan, tmp_path / "backup") == (
        "Recovery plan\n"
        f"  metadata directory: {metadata_dir}\n"
        f"  backup destination: {tmp_path / 'backup'}\n"
        "  last offset:        27\n"
        "  leader epoch:       5\n"
        "  snapshot:           00000000000000000028-0000000005.checkpoint\n"
        "  segments to delete: 00000000000000000000.log, "
        "00000000000000000004.log, which the snapshot is ahead of\n"
        "  file to delete:     leader-epoch-checkpoint, which Kafka rebuilds from "
        "the log\n"
        "  current voters:     0, 1\n"
        "  voter to keep:      0 (directory ID AAAAAAAAAAAAAAAAAAAQAA)\n"
        "  voters to remove:   1, in this order\n"
        f"  new log segment:    {metadata_dir / '00000000000000000028.log'} "
        "(existing, empty)\n"
        "  offsets to append:  28 to 28"
    )


class TestRecover:
    @pytest.fixture
    def backup_dir(self, tmp_path: Path) -> Path:
        return tmp_path / "backup"

    def assert_recovered(self, metadata_dir: Path, voter_id: int) -> None:
        summary = summarize(read_batches(list_segments(metadata_dir)))
        assert summary is not None
        assert summary.voters == voters_record(voter_id)

    def test_recovers_without_confirmation_given_yes(
        self,
        capsys: pytest.CaptureFixture[str],
        metadata_dir: Path,
        backup_dir: Path,
    ) -> None:
        original = _snapshot(metadata_dir)

        assert (
            main(
                [
                    "recover",
                    f"--metadata-dir={metadata_dir}",
                    f"--backup-dir={backup_dir}",
                    "--yes",
                ]
            )
            == 0
        )

        out = capsys.readouterr().out
        assert out.startswith("Recovery plan\n")
        assert "Recovery complete." in out
        assert "Start the controller on this node, node 0." in out
        self.assert_recovered(metadata_dir, 0)
        [backup] = backup_dir.iterdir()
        assert backup.name.startswith("__cluster_metadata-0.")
        assert _snapshot(backup / "__cluster_metadata-0") == original

    @pytest.mark.parametrize("answer", ["y", "Y", "yes", " Yes "])
    def test_recovers_after_confirmation(
        self,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
        metadata_dir: Path,
        backup_dir: Path,
        answer: str,
    ) -> None:
        prompts = []

        def confirm(prompt: str) -> str:
            prompts.append(prompt)
            return answer

        monkeypatch.setattr("builtins.input", confirm)

        assert (
            main(
                [
                    "recover",
                    f"--metadata-dir={metadata_dir}",
                    f"--backup-dir={backup_dir}",
                ]
            )
            == 0
        )

        assert prompts == ["Modify the metadata log as described above? [y/N] "]
        self.assert_recovered(metadata_dir, 0)

    @pytest.mark.parametrize("answer", ["n", "no", "", "yep"])
    def test_aborts_unless_confirmed(
        self,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
        metadata_dir: Path,
        backup_dir: Path,
        answer: str,
    ) -> None:
        original = _snapshot(metadata_dir)
        monkeypatch.setattr("builtins.input", lambda _: answer)

        assert (
            main(
                [
                    "recover",
                    f"--metadata-dir={metadata_dir}",
                    f"--backup-dir={backup_dir}",
                ]
            )
            == 1
        )

        assert capsys.readouterr().err == "Aborted, nothing was modified.\n"
        assert _snapshot(metadata_dir) == original
        assert not backup_dir.exists()

    def test_aborts_on_end_of_input(
        self,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
        metadata_dir: Path,
        backup_dir: Path,
    ) -> None:
        original = _snapshot(metadata_dir)

        def end_of_input(_: str) -> str:
            raise EOFError

        monkeypatch.setattr("builtins.input", end_of_input)

        assert (
            main(
                [
                    "recover",
                    f"--metadata-dir={metadata_dir}",
                    f"--backup-dir={backup_dir}",
                ]
            )
            == 1
        )

        assert _snapshot(metadata_dir) == original

    def test_warns_about_unverified_directory_id_given_node_id(
        self,
        caplog: pytest.LogCaptureFixture,
        metadata_dir: Path,
        backup_dir: Path,
    ) -> None:
        (metadata_dir.parent / "meta.properties").unlink()

        with caplog.at_level(logging.WARNING):
            assert (
                main(
                    [
                        "recover",
                        f"--metadata-dir={metadata_dir}",
                        f"--backup-dir={backup_dir}",
                        "--node-id=2",
                        "--yes",
                    ]
                )
                == 0
            )

        assert "directory ID of the local replica was not verified" in caplog.text
        self.assert_recovered(metadata_dir, 2)

    def test_reports_error_without_modifying(
        self,
        capsys: pytest.CaptureFixture[str],
        metadata_dir: Path,
        backup_dir: Path,
    ) -> None:
        original = _snapshot(metadata_dir)
        (metadata_dir.parent / "meta.properties").unlink()

        assert (
            main(
                [
                    "recover",
                    f"--metadata-dir={metadata_dir}",
                    f"--backup-dir={backup_dir}",
                    "--yes",
                ]
            )
            == 1
        )

        assert capsys.readouterr().err.startswith("error: Found no ")
        assert _snapshot(metadata_dir) == original
        assert not backup_dir.exists()
