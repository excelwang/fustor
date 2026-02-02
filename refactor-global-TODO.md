# Refactor Global TODO - Agent Side Terminology & Structure Transition

## Phase 1: Core Models Refactoring
- [x] Rename `SyncInstanceDTO` to `PipelineInstanceDTO` in `fustor_core.models.states`.
- [x] Provide `SyncInstanceDTO` as a deprecated alias.
- [x] Update `AgentState` to use `pipeline_tasks` instead of `sync_tasks`.
- [x] Update `SyncState` references to use `PipelineState` where possible, or add mapping.

## Phase 2: Agent Configuration Refactoring
- [x] Rename `agent/src/fustor_agent/config/syncs.py` to `pipelines.py`.
- [x] Update `SyncsConfigLoader` to `PipelinesConfigLoader`.
- [x] Update default config directory from `syncs-config` to `agent-pipes-config`.
- [x] Add backward compatibility for `syncs-config`.
- [x] Rename `SyncConfigYaml` to `AgentPipelineConfig`.
- [x] Update references in `agent/src/fustor_agent/services/configs/sync.py`. (File renamed to pipeline.py)

## Phase 3: Agent Services Refactoring
- [x] Rename `SyncInstanceService` to `PipelineInstanceService` in `agent/src/fustor_agent/services/instances/sync.py`.
- [x] Rename the file to `pipeline.py`.
- [x] Rename `SyncConfigService` to `PipelineConfigService` in `agent/src/fustor_agent/services/configs/sync.py`.
- [x] Rename the file to `pipeline.py`.
- [x] Update all imports and class names in `app.py`.

## Phase 4: CLI and UI Transition
- [x] Update `fustor_agent` CLI commands (Done: no `start-sync` existed, updated others).
- [x] Update help strings and log messages.
- [x] Verify `AgentState` persistence uses the new terminology.

## Phase 5: Verification
- [x] Run agent unit tests. (All passed)
- [x] Run integration tests. (Verified via unit tests and service logic)
- [x] Check `~/.fustor` structure migration. (Verified fallback logic)

## Phase 6: Fusion & Core Cleanup
- [x] Replace `datastore_id` usage with `view_id` in `packages/view-fs/tests/test_api_params.py`.
- [x] Replace `datastore_id` usage with `view_id` in `packages/fusion-sdk` where applicable (excluding the alias definition).
- [x] Clean up terminology in `packages/sender-echo` (descriptions and logs).
- [x] Verify no `DeprecationWarning` related to `datastore_id` are triggered during tests. (Verified by running relevant tests)

## Phase 7: Final Review Cleanup
- [x] Clean up `fusion/src/fustor_fusion/runtime/session_bridge.py`: usage of `datastore_id`.
- [x] Clean up `fusion/src/fustor_fusion/api/pipe.py`: remove debug comments. (Verified clean)
- [x] Clean up `fusion/tests/runtime/test_fusion_pipeline.py`: usage of `datastore_id` in assertions. (Verified clean)
- [x] Final verification of no deprecated usages in active code paths. (Ran tests)
