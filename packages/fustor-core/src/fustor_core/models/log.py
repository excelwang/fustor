from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional

class LogEntry(BaseModel):
    """定义了单条日志记录的结构化模型"""
    timestamp: datetime = Field(..., alias="ts")
    level: str
    component: str = Field(..., alias="source")
    message: str = Field(..., alias="msg")
    line_number: int # 用于分页的关键字段