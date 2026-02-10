# extensions/source-fs/tests/test_snapshot_root_failure.py
"""
U7: 当根目录不可访问时，Snapshot 扫描的行为。

被测代码: extensions/source-fs/src/fustor_source_fs/scanner.py → FSScanner.scan_snapshot()

验证重点:
1. 根目录不存在时，scan_snapshot 抛出 OSError (由调用方决定如何处理)。
2. 权限被拒时，scan_snapshot 抛出 OSError。
3. 调用方 (如 AgentPipe) 负责捕获并进行自愈。

Spec: 05-Stability.md §1.2 — "单个文件失败不影响任务"，但根目录不可达是全局性错误。
"""
import pytest
import os
from fustor_source_fs.scanner import FSScanner


class TestSnapshotRootFailure:

    def test_snapshot_on_nonexistent_root_raises_error(self):
        """
        验证: 根目录不存在时，scan_snapshot 抛出 OSError。
        调用方应捕获此异常并决定是否重试。
        """
        scanner = FSScanner(root="/nonexistent/path/that/does/not/exist")

        with pytest.raises(OSError, match="missing or inaccessible"):
            list(scanner.scan_snapshot(
                event_schema="test",
                batch_size=100
            ))

    def test_snapshot_on_permission_denied_root_raises_error(
        self, tmp_path
    ):
        """
        验证: 权限被拒的根目录，scan_snapshot 抛出 OSError。
        """
        restricted_dir = tmp_path / "restricted"
        restricted_dir.mkdir()
        (restricted_dir / "file.txt").write_text("hello")
        os.chmod(str(restricted_dir), 0o000)

        try:
            scanner = FSScanner(root=str(restricted_dir))

            with pytest.raises(OSError, match="missing or inaccessible"):
                list(scanner.scan_snapshot(
                    event_schema="test",
                    batch_size=100
                ))
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
