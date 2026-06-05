"""storage.atomic_write 回归测试。"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from sim_dji_cloud.storage.atomic_write import atomic_write_text


def test_writes_full_content(tmp_path: Path) -> None:
    target = tmp_path / "manifest.json"
    atomic_write_text(target, '{"a": 1}')
    assert target.read_text() == '{"a": 1}'


def test_overwrites_existing_file(tmp_path: Path) -> None:
    target = tmp_path / "manifest.json"
    target.write_text("OLD")
    atomic_write_text(target, "NEW")
    assert target.read_text() == "NEW"


def test_no_tmp_leak_on_success(tmp_path: Path) -> None:
    target = tmp_path / "manifest.json"
    atomic_write_text(target, "x")
    assert not (tmp_path / "manifest.json.tmp").exists()


def test_crash_between_fsync_and_rename_keeps_old_version(tmp_path: Path) -> None:
    """模拟 os.replace 之前进程被 kill：原文件不应被破坏（rename 没发生）。"""
    target = tmp_path / "manifest.json"
    target.write_text("OLD")

    with patch("sim_dji_cloud.storage.atomic_write.os.replace",
               side_effect=KeyboardInterrupt):
        with pytest.raises(KeyboardInterrupt):
            atomic_write_text(target, "NEW")

    assert target.read_text() == "OLD"


def test_creates_parent_dirs(tmp_path: Path) -> None:
    target = tmp_path / "deep" / "nest" / "manifest.json"
    atomic_write_text(target, "hi")
    assert target.read_text() == "hi"
