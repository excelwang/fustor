from typing import Dict, Optional, Any, List

class BaseInstanceService:
    """实例服务的抽象基类。"""
    def __init__(self):
        self.pool: Dict[str, Any] = {}

    def get_instance(self, id: str) -> Optional[Any]:
        """按ID查找一个运行时实例，找不到时返回None。"""
        return self.pool.get(id)

    def list_instances(self) -> List[Any]:
        """列出所有运行时实例。"""
        return list(self.pool.values())