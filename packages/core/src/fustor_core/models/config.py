from pydantic import BaseModel, Field, field_validator, RootModel, ConfigDict
from typing import List, Optional, Union, TypeAlias, Dict, Any
from fustor_core.exceptions import ConfigError, NotFoundError

class PasswdCredential(BaseModel):
    model_config = ConfigDict(extra='forbid')

    user: str = Field(..., description="用户名")
    passwd: Optional[str] = Field(None, description="密码")

    def to_base64(self) -> str:
        """为HTTP Basic Auth生成Base64编码的字符串。"""
        import base64
        auth_str = f"{self.user}:{self.passwd or ''}"
        return base64.b64encode(auth_str.encode('utf-8')).decode('utf-8')

    def _get_hashable_data(self):
        return ("PasswdCredential", self.user, self.passwd)

    def __hash__(self):
        return hash(self._get_hashable_data())

    def __eq__(self, other):
        if not isinstance(other, PasswdCredential):
            return NotImplemented
        return self._get_hashable_data() == other._get_hashable_data()

class ApiKeyCredential(BaseModel):
    model_config = ConfigDict(extra='forbid')

    user: Optional[str] = Field(None, description="用户名")
    key: str = Field(..., description="api key")

    def _get_hashable_data(self):
        return ("ApiKeyCredential", self.user, self.key)

    def __hash__(self):
        return hash(self._get_hashable_data())

    def __eq__(self, other):
        if not isinstance(other, ApiKeyCredential):
            return NotImplemented
        return self._get_hashable_data() == other._get_hashable_data()

# Reordered Union to prioritize the more specific ApiKeyCredential
Credential: TypeAlias = Union[ApiKeyCredential, PasswdCredential]

class FieldMapping(BaseModel):
    to: str = Field(..., description="供给字段")
    source: List[str] = Field(..., description="来源字段")
    required: bool = Field(default=False, description="是否为必填字段")

class SourceConfig(BaseModel):
    driver: str
    uri: str
    credential: Credential
    max_queue_size: int = Field(default=1000, gt=0, description="事件缓冲区的最大尺寸")
    max_retries: int = Field(default=10, gt=0, description="驱动在读取事件失败时的最大重试次数")
    retry_delay_sec: int = Field(default=5, gt=0, description="驱动重试前的等待秒数")
    disabled: bool = Field(default=True, description="是否禁用此配置")
    validation_error: Optional[str] = Field(None, exclude=True)
    driver_params: Dict[str, Any] = Field(default_factory=dict, description="驱动专属参数")

class SenderConfig(BaseModel):
    """
    Configuration for a Sender (formerly called Pusher).
    
    Senders are responsible for sending events to downstream systems (e.g., Fusion).
    """
    driver: str
    uri: str = Field(..., alias="endpoint", description="目标端点URL")  # 'endpoint' alias for backward compat
    credential: Credential
    batch_size: int = Field(default=1000, ge=1, description="每批消息最大条目")
    max_retries: int = Field(default=10, gt=0, description="推送失败时的最大重试次数")
    retry_delay_sec: int = Field(default=5, gt=0, description="推送重试前的等待秒数")
    timeout_sec: int = Field(default=30, gt=0, description="网络请求超时(秒)")
    disabled: bool = Field(default=True, description="是否禁用此配置")
    validation_error: Optional[str] = Field(None, exclude=True)
    driver_params: Dict[str, Any] = Field(default_factory=dict, description="驱动专属参数")
    # API version for FusionSDK compatibility ('v1' or 'v2')
    api_version: str = Field(default='v2', pattern='^v[12]$', description="API version (v1 or v2)")
    
    model_config = ConfigDict(populate_by_name=True)  # Allow using 'endpoint' or 'uri'
    
    @property
    def endpoint(self) -> str:
        """Alias for uri for backward compatibility."""
        return self.uri


class SyncConfig(BaseModel):
    """
    Configuration for a Sync task that connects a Source to a Sender.
    
    The 'sender' field specifies which sender configuration to use.
    """
    source: str = Field(..., description="数据源的配置 ID")
    sender: str = Field(..., description="发送器的配置 ID")
    disabled: bool = Field(default=True, description="是否禁用此配置")
    fields_mapping: List[FieldMapping] = Field(default_factory=list)
    # Consistency-related intervals (Section 7 of CONSISTENCY_DESIGN)
    audit_interval_sec: int = Field(default=600, ge=0, description="审计扫描间隔(秒)，0表示禁用，默认10分钟")
    sentinel_interval_sec: int = Field(default=120, ge=0, description="哨兵巡检间隔(秒)，0表示禁用，默认2分钟")
    heartbeat_interval_sec: int = Field(default=10, ge=1, description="心跳间隔(秒)，默认10秒")


class SourceConfigDict(RootModel[Dict[str, SourceConfig]]):
    root: Dict[str, SourceConfig] = Field(default_factory=dict)

class SenderConfigDict(RootModel[Dict[str, SenderConfig]]):
    root: Dict[str, SenderConfig] = Field(default_factory=dict)

class SyncConfigDict(RootModel[Dict[str, SyncConfig]]):
    root: Dict[str, SyncConfig] = Field(default_factory=dict)

class AppConfig(BaseModel):
    """
    Application configuration containing sources, senders, and syncs.
    """
    sources: SourceConfigDict = Field(default_factory=SourceConfigDict)
    senders: SenderConfigDict = Field(default_factory=SenderConfigDict)
    syncs: SyncConfigDict = Field(default_factory=SyncConfigDict)

    def get_sources(self) -> Dict[str, SourceConfig]:
        return self.sources.root
    
    def get_senders(self) -> Dict[str, SenderConfig]:
        """Get all sender configurations."""
        return self.senders.root

    def get_syncs(self) -> Dict[str, SyncConfig]:
        return self.syncs.root
    
    def get_source(self, id: str) -> Optional[SourceConfig]:
        return self.get_sources().get(id)
    
    def get_sender(self, id: str) -> Optional[SenderConfig]:
        """Get sender config by ID."""
        return self.get_senders().get(id)

    def get_sync(self, id: str) -> Optional[SyncConfig]:
        return self.get_syncs().get(id)
    
    def add_source(self, id: str, config: SourceConfig) -> SourceConfig:
        config_may = self.get_source(id)
        if config_may:
            raise ConfigError(f"Source config with name '{id}' already exists.")
        self.get_sources()[id] = config
        return config

    def add_sender(self, id: str, config: SenderConfig) -> SenderConfig:
        """Add sender config."""
        config_may = self.get_sender(id)
        if config_may:
            raise ConfigError(f"Sender config with name '{id}' already exists.")
        self.get_senders()[id] = config
        return config

    def add_sync(self, id: str, config: SyncConfig) -> SyncConfig:
        config_may = self.get_sync(id)
        if config_may:
            raise ConfigError(f"Sync config with id '{id}' already exists.")
        
        # Dependency check
        if not self.get_source(config.source):
            raise NotFoundError(f"Dependency source '{config.source}' not found.")
        if not self.get_sender(config.sender):
            raise NotFoundError(f"Dependency sender '{config.sender}' not found.")
        
        self.get_syncs()[id] = config
        return config
    
    def delete_source(self, id: str) -> SourceConfig:
        config = self.get_source(id)
        if not config:
            raise NotFoundError(f"Source config with id '{id}' not found.")
        
        # Delete dependent syncs first
        sync_ids_to_delete = [sync_id for sync_id, cfg in self.get_syncs().items() if cfg.source == id]
        for sync_id in sync_ids_to_delete:
            self.delete_sync(sync_id)
            
        return self.get_sources().pop(id)
    
    def delete_sender(self, id: str) -> SenderConfig:
        """Delete sender config."""
        config = self.get_sender(id)
        if not config:
            raise NotFoundError(f"Sender config with id '{id}' not found.")
        
        # Delete dependent syncs first
        sync_ids_to_delete = [sync_id for sync_id, cfg in self.syncs.root.items() if cfg.sender == id]
        for sync_id in sync_ids_to_delete:
            self.delete_sync(sync_id)
            
        return self.get_senders().pop(id)
    
    def delete_sync(self, id: str) -> SyncConfig:
        config = self.get_sync(id)
        if not config:
            raise NotFoundError(f"Sync config with id '{id}' not found.")
        return self.get_syncs().pop(id)

    def check_sync_is_disabled(self, id: str) -> bool:
        config = self.get_sync(id)
        if not config:
            raise NotFoundError(f"Sync with id '{id}' not found.")
        
        if config.disabled:
            return True
        
        source_config = self.sources.root.get(config.source)
        if not source_config:
            raise NotFoundError(f"Dependency source '{config.source}' not found for sync '{id}'.")
            
        sender_config = self.senders.root.get(config.sender)
        if not sender_config:
            raise NotFoundError(f"Dependency sender '{config.sender}' not found for sync '{id}'.")
            
        return source_config.disabled or sender_config.disabled