# 旧代码清理计划

## 扫描结果摘要

### 1. fustor_common 包
- ✅ **状态**: 仅在测试中使用，已有废弃警告
- **位置**: `packages/common/`
- **使用者**: `packages/common/tests/test_common_models.py`
- **决策**: **保留** - 作为向后兼容层，有测试覆盖

### 2. 术语: pusher → sender
- ⚠️ **状态**: 大量残留，需要系统清理
- **影响范围**: 
  - `SyncInstance` 内部变量 (47 处)
  - `SyncConfig.pusher` 字段
  - 服务类参数名
  - 配置文件字段

## 清理策略

### 选项 A: 激进清理 (推荐)
**彻底移除 pusher 术语，完全使用 sender**

优点:
- 代码库术语统一
- 减少混淆
- 未来维护成本低

缺点:
- 需要更新集成测试配置
- 需要迁移指南
- 存量用户需要更新配置

### 选项 B: 保守清理
**保留配置字段向后兼容，仅重命名内部代码**

优点:
- 对用户影响小
- 配置文件无需改动

缺点:
- 代码库仍有术语混用
- 增加长期维护成本

## 推荐方案: 激进清理

### Phase 1: 核心代码清理

#### 1.1 SyncConfig 字段重命名
```python
# agent/src/fustor_agent/config/syncs.py

# 当前
class SyncConfig(BaseModel):
    pusher: str  # ❌

# 修改为
class SyncConfig(BaseModel):
    sender: str  # ✅
    
    # 向后兼容: 支持 pusher 字段自动迁移
    @field_validator('sender', mode='before')
    @classmethod
    def migrate_pusher_field(cls, v, info):
        # 如果有 pusher 字段，使用它作为 sender
        if v is None and 'pusher' in info.data:
            warnings.warn(
                "SyncConfig field 'pusher' is deprecated, use 'sender' instead",
                DeprecationWarning
            )
            return info.data['pusher']
        return v
```

#### 1.2 SyncInstance 内部变量重命名
```python
# agent/src/fustor_agent/runtime/sync.py

class SyncInstance:
    def __init__(
        self,
        # 参数名改为 sender_*
        sender_config: SenderConfig,  # ✅ 原 pusher_config
        sender_driver_service,        # ✅ 原 pusher_driver_service
        sender_schema,                 # ✅ 原 pusher_schema
    ):
        self.sender_config = sender_config
        self.sender_driver_instance = ...
        # 移除所有 self.pusher_* 引用
```

#### 1.3 SyncInstanceService 清理
```python
# agent/src/fustor_agent/services/instances/sync.py

class SyncInstanceService:
    def __init__(
        self,
        sender_config_service,    # ✅ 原 pusher_config_service
        sender_driver_service,    # ✅ 原 pusher_driver_service
    ):
        self.sender_config_service = sender_config_service
        self.sender_driver_service = sender_driver_service
```

### Phase 2: 配置文件迁移

#### 2.1 配置加载器更新
```python
# agent/src/fustor_agent/services/configs/base.py

# 更新 config_type 支持
if self.config_type in ['source', 'sender']:  # ✅ 原 'pusher'
    ...
```

#### 2.2 YAML 配置兼容性
```yaml
# syncs-config/example.yaml

# 新格式 (推荐)
id: sync-1
source: fs-source
sender: http-sender  # ✅

# 旧格式 (自动迁移，显示警告)
id: legacy-sync
source: fs-source
pusher: http-sender  # ⚠️ 废弃警告
```

### Phase 3: 测试更新

#### 3.1 单元测试
- 更新所有使用 `pusher` 的测试
- 添加向后兼容测试验证警告

#### 3.2 集成测试
- 更新 `it/conftest.py` 配置生成
- `pushers-config.yaml` → `senders-config.yaml`

### Phase 4: 文档更新

#### 4.1 迁移指南
创建 `docs/MIGRATION_PUSHER_TO_SENDER.md`:
```markdown
# Pusher → Sender 迁移指南

## 配置文件变更

### syncs-config/*.yaml
将 `pusher` 字段改为 `sender`:
- pusher: my-pusher  # ❌ 旧
+ sender: my-sender  # ✅ 新

### pushers-config.yaml
重命名为 `senders-config.yaml`，内容格式不变

## 自动迁移

运行迁移工具:
```bash
fustor-agent migrate-config
```
```

#### 4.2 API 文档更新
- README.md
- ARCHITECTURE_V2.md
- CONFIGURATION.md

## 实施时间表

| 阶段 | 任务 | 预估时间 |
|------|------|----------|
| Phase 1 | 核心代码清理 | 2-3 小时 |
| Phase 2 | 配置兼容层 | 1-2 小时 |
| Phase 3 | 测试更新 | 2-3 小时 |
| Phase 4 | 文档更新 | 1 小时 |
| **总计** | | **6-9 小时** |

## 风险评估

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| 破坏现有配置 | 高 | 添加字段迁移验证器 |
| 测试失败 | 中 | 每步运行测试验证 |
| 用户混淆 | 低 | 提供清晰的迁移文档 |

## 验证清单

- [ ] 所有单元测试通过 (401 passed)
- [ ] 集成测试通过 (24 个)
- [ ] 向后兼容测试验证警告正确显示
- [ ] 文档更新完成
- [ ] 迁移指南已创建

## 回滚计划

如遇问题，可通过以下步骤回滚：
1. `git revert` 相关提交
2. 恢复原有术语
3. 保留兼容层代码

## 决策请求

**推荐执行激进清理方案**，理由:
1. 术语统一是长期收益
2. 现在是最佳时机（V2 架构刚完成）
3. 用户群还不大，迁移成本低

是否批准开始清理？
