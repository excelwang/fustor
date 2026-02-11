# extensions/source-fs/tests/test_snapshot_root_failure.py
"""
U7: 当根目录不可访问时，Snapshot 扫描的行为。

被测代码: extensions/source-fs/src/fustor_source_fs/scanner.py → FSScanner.scan_snapshot()

验证重点:
1. 根目录不存在时，scan_snapshot 不再抛出致命异常，而是优雅跳过（由调用方日志记录）。
2. 权限被拒时，scan_snapshot 同样优雅跳过。
3. 保证 Agent 整体流程不因环境临时不可用而崩溃。

Spec: 05-Stability.md §1.2 — "单个文件失败不影响任务"。
"""
import pytest
import os
from fustor_source_fs.scanner import FSScanner


class TestSnapshotRootFailure:

    def test_snapshot_on_nonexistent_root_gracefully_skips(self):
        """
        验证: 根目录不存在时，scan_snapshot 优雅跳过，返回空迭代。
        """
        scanner = FSScanner(root="/nonexistent/path/that/does/not/exist")

        # 之前这里是 raises(OSError)，现在应该是正常结束
        events = list(scanner.scan_snapshot(
            event_schema="test",
            batch_size=100
        ))
        assert len(events) == 0

    def test_snapshot_on_permission_denied_root_gracefully_skips(
        self, tmp_path
    ):
        """
        验证: 权限被拒的根目录，scan_snapshot 优雅跳过。
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
            assert len(events) == 0
        finally:
            os.chmod(str(restricted_dir), 0o755)

    def test_snapshot_on_valid_root_works_normally(self, tmp_path):
        """
        验证: 正常根目录下 scan_snapshot 不抛异常。
        """
        (tmp_path / "hello.txt").write_text("world")
        scanner = FSScanner(root=str(tmp_path))

        events = list(scanner.scan_snapshot(
            event_schema="test",
            batch_size=100
        ))

        assert len(events) > 0, "有效根目录应产生事件"
        assert scanner.engine.error_count == 0, \
            "无错误时 error_count 应为 0"