import re
import os
import logging # 导入logging模块
from datetime import datetime
from typing import List, Optional, Generator

from .. import CONFIG_DIR # 移除logger导入
from fustor_core.models.log import LogEntry

# 正则表达式，用于解析格式为 'YYYY-MM-DD HH:MM:SS,ms - component - LEVEL - message' 的日志行
LOG_PATTERN = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\s*-\s*"
    r"(?P<component>[\w\.]+)\s*-\s*"
    r"(?P<level>\w+)\s*-\s*"
    r"(?P<message>.*)$"
)

LOG_FILE_PATH = os.path.join(CONFIG_DIR, 'fustor_agent.log')



        

class LogService:
    """负责所有日志文件的读取、解析和筛选逻辑。"""
    def __init__(self):
        self.logger = logging.getLogger("fustor_agent") # 在这里获取logger实例

    def _parse_line(self, line: str, line_number: int) -> Optional[LogEntry]:
        """解析单行日志文本，返回结构化的LogEntry对象。"""
        match = LOG_PATTERN.match(line)
        if not match:
            return None
        
        data = match.groupdict()
        try:
            # Pydantic模型会自动处理别名 'ts', 'source', 'msg'
            return LogEntry(
                ts=datetime.strptime(data['timestamp'], "%Y-%m-%d %H:%M:%S,%f"),
                level=data['level'],
                source=data['component'],
                msg=data['message'],
                line_number=line_number
            )
        except (ValueError, KeyError):
            return None

    def get_logs(
        self,
        limit: int = 100,
        level: Optional[str] = None,
        component: Optional[str] = None,
        before_line: Optional[int] = None
    ) -> List[LogEntry]:
        """
        获取日志条目列表，支持过滤和分页。
        从后向前读取文件，以实现“最新的在最前”。
        """
        if not os.path.exists(LOG_FILE_PATH):
            self.logger.warning(f"日志文件未找到: {LOG_FILE_PATH}") # 使用self.logger
            return []

        results: List[LogEntry] = []
        try:
            with open(LOG_FILE_PATH, 'r', encoding='utf-8') as f:
                all_lines = f.readlines()
                
                # Iterate from the end of the file backwards
                for i in range(len(all_lines) - 1, -1, -1):
                    line_num = i + 1
                    line_content = all_lines[i].strip()

                    # Apply before_line filter
                    if before_line is not None and line_num >= before_line:
                        continue

                    entry = self._parse_line(line_content, line_num)
                    if not entry:
                        continue

                    # Apply other filters
                    if level and entry.level.upper() != level.upper():
                        continue
                    if component and not entry.component.startswith(component):
                        continue
                    
                    results.append(entry)
                    if len(results) >= limit:
                        break
            
            return results
        except Exception as e:
            self.logger.error(f"读取或解析日志文件时出错: {e}", exc_info=True) # 使用self.logger
            return []