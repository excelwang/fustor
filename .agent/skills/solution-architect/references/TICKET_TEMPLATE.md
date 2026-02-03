# Task Document Template

此模板用于定义 Level 3 Ticket (工单)。

```markdown
# Ticket: [ID] [Title]
> Status: Draft | Backlog | In Progress | Paused | Reviewing | Done
> Owner: AI Assistant
> Created: 202X-XX-XX

## 1. Goal (目标)
简述本次变更的业务价值。

## 2. References (依据)
> 此任务必须基于以下 Spec 执行：
- [ ] Spec: `specs/10-DOMAIN_XXX.md`
- [ ] Prerequisites: Ticket ID [If Any]

## 3. Scope & Deliverables (范围与交付物)
- [ ] Logic: `src/core/pipeline.py`
- [ ] **Contract Test**: `it/specs/consistency/ticket_001_contract.py` (Skeleton Created)
- [ ] **Mock**: `tests/fixtures/mocks/mock_pipeline.py` (If required by others)
- [ ] Unit Test: `tests/unit/core/test_pipeline.py` (Optional)

## 4. Work Breakdown (执行步骤)
按依赖顺序拆解为原子步骤（对应 Atomic Commits）：
1. **Step 1**: Define Interface (Interface-first design)
2. **Step 2**: Implement Core Logic
3. **Step 3**: Update Tests

## 5. Acceptance Criteria (验收标准)
> Review Gate Checklist
- [ ] Feature Parity: 旧版 Pusher 逻辑是否完全覆盖？
- [ ] Test Pass: `uv run pytest it/xxx` 全绿。
```
