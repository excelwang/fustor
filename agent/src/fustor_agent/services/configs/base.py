# src/fustor_agent/services/configs/base.py
import logging
from typing import Dict, Optional, TypeVar, Generic, Any

from fustor_agent import update_app_config_file
from fustor_core.models.config import AppConfig
from fustor_agent.services.common import config_lock
from fustor_agent.services.instances.sync import SyncInstanceService
from fustor_core.models.states import SyncState
from fustor_core.exceptions import ConfigError, NotFoundError, ConflictError

logger = logging.getLogger("fustor_agent")

T = TypeVar('T') # 用于泛型类型提示

class BaseConfigService(Generic[T]):
    """
    一个通用的配置管理服务基类，封装了对配置项的CRUD、启用/禁用等通用逻辑。
    """
    def __init__(
        self,
        app_config: AppConfig,
        sync_instance_service: Optional[SyncInstanceService],
        config_type: str
    ):
        """
        初始化基类。
        
        Args:
            app_config: 主应用配置实例。
            sync_instance_service: 同步任务实例服务，用于处理依赖关系。
            config_type: 配置类型的小写字符串 (例如 'source', 'pusher', 'sync')。
        """
        self.app_config = app_config
        self.sync_instance_service = sync_instance_service
        self.config_type = config_type
        self.config_type_capitalized = config_type.capitalize()

    def _get_config_dict(self) -> Dict[str, T]:
        """动态获取特定类型的配置字典。"""
        method_name = f"get_{self.config_type}s"
        return getattr(self.app_config, method_name, lambda: {})()

    def _add_config_to_app(self, id: str, config: T):
        """动态调用AppConfig中对应的添加方法。"""
        method_name = f"add_{self.config_type}"
        return getattr(self.app_config, method_name)(id, config)

    def _delete_config_from_app(self, id: str) -> T:
        """动态调用AppConfig中对应的删除方法。"""
        method_name = f"delete_{self.config_type}"
        return getattr(self.app_config, method_name)(id)

    def list_configs(self) -> Dict[str, T]:
        """列出所有当前类型的配置项。"""
        return self._get_config_dict()

    def get_config(self, id: str) -> Optional[T]:
        """通过ID查找一个配置项。"""
        return self.list_configs().get(id)

    async def add_config(self, id: str, config: T) -> T:
        """添加一个新的配置项并持久化到文件。"""
        async with config_lock:
            self._add_config_to_app(id, config)
            update_app_config_file()
            logger.info(f"{self.config_type_capitalized} '{id}' configuration added.")
            return config

    async def update_config(self, id: str, updates: Dict[str, Any]) -> T:
        """
        更新一个配置项并持久化到文件。
        如果 disabled 状态发生变化，则通知受影响的实例。
        """
        async with config_lock:
            conf = self.get_config(id)
            if not conf:
                raise NotFoundError(f"{self.config_type_capitalized} config '{id}' not found.")

            old_disabled_status = conf.disabled
            
            # Apply updates
            for key, value in updates.items():
                setattr(conf, key, value)
            
            update_app_config_file()
            logger.info(f"{self.config_type_capitalized} '{id}' configuration updated.")

            # Check if disabled status changed and notify if necessary
            if 'disabled' in updates and updates['disabled'] != old_disabled_status:
                status = "disabled" if updates['disabled'] else "enabled"
                if self.sync_instance_service:
                    reason = f"Dependency {self.config_type_capitalized} '{id}' configuration was {status}."
                    if self.config_type in ['source', 'pusher']:
                        await self.sync_instance_service.mark_dependent_syncs_outdated(self.config_type, id, reason, updates)
                    elif self.config_type == 'sync':
                        instance = self.sync_instance_service.get_instance(id)
                        if instance and instance.state not in {SyncState.STOPPED, SyncState.ERROR}:
                            instance._set_state(SyncState.RUNNING_CONF_OUTDATE, reason)
            return conf

    async def delete_config(self, id: str) -> T:
        """Deletes a configuration item after checking for dependencies."""
        # Check for dependent sync tasks before deleting.
        dependent_syncs = [
            sync_id for sync_id, sync_config in self.app_config.get_syncs().items()
            if (self.config_type == 'source' and sync_config.source == id) or \
               (self.config_type == 'pusher' and sync_config.pusher == id)
        ]

        if dependent_syncs:
            raise ConflictError(
                f"{self.config_type_capitalized} '{id}' cannot be deleted because it is used by the following sync tasks: {', '.join(dependent_syncs)}. "
                f"Please delete these tasks first."
            )

        async with config_lock:
            # Stop the instance if it's a sync task itself being deleted.
            if self.sync_instance_service and self.config_type == 'sync':
                await self.sync_instance_service.stop_one(id)
            
            conf = self._delete_config_from_app(id)
            update_app_config_file()
            logger.info(f"{self.config_type_capitalized} '{id}' configuration deleted.")
        
        return conf

    async def disable(self, id: str):
        """禁用一个配置项。"""
        await self.update_config(id, {'disabled': True})

    async def enable(self, id: str):
        """启用一个配置项。"""
        await self.update_config(id, {'disabled': False})