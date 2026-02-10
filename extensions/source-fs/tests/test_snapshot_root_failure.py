# extensions/source-fs/tests/test_snapshot_root_failure.py
"""
U7: 当根目录不可访问时，Snapshot 扫描的行为。

被测代码: extensions/source-fs/src/fustor_source_fs/scanner.py → FSScanner.scan_snapshot()

已知 BUG: snapshot_worker 中 os.stat(root) 的 OSError 只被 log，不被传播。
error_count 属性是 RecursiveScanner 级别的 worker 错误计数，
但 snapshot_worker 内部的 per-directory error 不增加该计数。
"""
import pytest
import os
from fustor_source_fs.scanner import FSScanner


class TestSnapshotRootFailure:

    def test_snapshot_on_nonexistent_root_returns_empty(self):
        """
        验证修复后: 根目录不存在时，返回空列表，但 error_count > 0。
        """
        scanner = FSScanner(root="/nonexistent/path/that/does/not/exist")

        events = list(scanner.scan_snapshot(
            event_schema="test",
            batch_size=100
        ))

        assert len(events) == 0, "不存在的根目录应产生 0 个事件"
        # 修复验证: error_count 应增加
        assert scanner.engine.error_count > 0, \
            "error_count 应计入 snapshot_worker 内部的 OSError"

    def test_snapshot_on_permission_denied_root_reports_error(
        self, tmp_path
    ):
        """
        验证修复后: 权限被拒的根目录应报告错误。
        """
        restricted_dir = tmp_path / "restricted"
        restricted_dir.mkdir()
        (restricted_dir / "file.txt").write_text("hello")
        os.chmod(str(restricted_dir), 0o000)

        try:
            scanner = FSScanner(root=str(restricted_dir))
            events = list(scanner.scan_snapshot(
                event_schema="test",
                batch_size=100
            ))

            # 修复验证: error_count 应增加
            assert scanner.engine.error_count > 0, \
                "PermissionError 应增加 error_count"
        finally:
            os.chmod(str(restricted_dir), 0o755)
