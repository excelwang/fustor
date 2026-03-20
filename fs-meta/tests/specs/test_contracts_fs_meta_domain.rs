//! L1 Contract: fs-meta domain constitution contracts
//!
//! ASSERTION ROUTE: Provide traceable verification anchors for domain-level cross-module contracts.

use crate::common::{
    assert_l1_contract, assert_runtime_orchestrated_multiprocess_config_apply_e2e,
    assert_single_entrypoint_distributed_apply_e2e,
};
use capanix_app_sdk::runtime::{ConfigValue, NodeId};
use serde_yaml::{Mapping, Value};

fn read_fs_meta_spec_file(path: &str) -> String {
    let root = crate::path_support::fs_meta_root();
    let normalized = path.strip_prefix("fs-meta/").unwrap_or(path);
    let resolved = match normalized {
        "Cargo.toml" => root.join("app/Cargo.toml"),
        _ if normalized.starts_with("src/") => root.join("app").join(normalized),
        _ => root.join(normalized),
    };
    std::fs::read_to_string(resolved).expect("read fs-meta spec file")
}

fn load_yaml(path: &str) -> Value {
    let yaml = read_fs_meta_spec_file(path);
    serde_yaml::from_str(&yaml).expect("parse fs-meta yaml")
}

fn assert_domain_fs_meta_contract_test(contract_id: &str, fn_name: &str) {
    let domain_tests =
        read_fs_meta_spec_file("fs-meta/tests/specs/test_contracts_fs_meta_domain.rs");
    let marker = format!("@verify_spec(\"{}\"", contract_id);
    assert!(
        domain_tests.contains(&marker),
        "domain test file should contain verify marker for {}",
        contract_id
    );
    let fn_sig = format!("fn {}", fn_name);
    assert!(
        domain_tests.contains(&fn_sig),
        "domain test file should contain function {}",
        fn_name
    );
}

fn root_mapping(doc: &Value) -> &Mapping {
    doc.as_mapping().expect("yaml root mapping")
}

fn apps(doc: &Value) -> &Vec<Value> {
    root_mapping(doc)
        .get(Value::String("apps".to_string()))
        .and_then(Value::as_sequence)
        .expect("apps sequence")
}

fn find_app<'a>(doc: &'a Value, id_or_app: &str) -> &'a Mapping {
    apps(doc)
        .iter()
        .find_map(|v| {
            let m = v.as_mapping()?;
            let app_id = m
                .get(Value::String("id".to_string()))
                .and_then(Value::as_str);
            let app_bin = m
                .get(Value::String("app".to_string()))
                .and_then(Value::as_str);
            (app_id == Some(id_or_app) || app_bin == Some(id_or_app)).then_some(m)
        })
        .expect("app by id/app")
}

fn app_name(app: &Mapping) -> &str {
    app.get(Value::String("app".to_string()))
        .and_then(Value::as_str)
        .or_else(|| {
            app.get(Value::String("id".to_string()))
                .and_then(Value::as_str)
        })
        .expect("app/id field")
}

fn app_count(doc: &Value, id_or_app: &str) -> usize {
    root_mapping(doc)
        .get(Value::String("apps".to_string()))
        .and_then(Value::as_sequence)
        .map(|apps| {
            apps.iter()
                .filter(|a| app_name(a.as_mapping().expect("app mapping")) == id_or_app)
                .count()
        })
        .unwrap_or(0)
}

// @verify_spec("CONTRACTS.DOMAIN_IDENTITY.DOMAIN_CONSTITUTION_SCOPE_DISCIPLINE", mode="system")
#[test]
fn test_domain_constitution_scope_discipline() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("domain-level specs focused on externally observable contracts"));
    assert!(l1.contains("DOMAIN_CONSTITUTION_SCOPE_DISCIPLINE"));
}

// @verify_spec("CONTRACTS.DOMAIN_IDENTITY.ROLE_BOUNDARY_ENFORCEMENT", mode="system")
#[test]
fn test_role_boundary_enforcement() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    let l2 = read_fs_meta_spec_file("fs-meta/specs/L2-ARCHITECTURE.md");
    let manifest = read_fs_meta_spec_file("fs-meta/app/Cargo.toml");
    assert!(l1.contains("one downstream app product container"));
    assert!(l2.contains("Single fs-meta App Boundary"));
    assert!(l2.contains("downstream app product container"));
    assert!(
        l2.contains("encapsulate source/sink semantics") && l2.contains("HTTP facade semantics")
    );
    assert!(manifest.contains("description = \"fs-meta application package\""));
    assert!(!manifest.contains("description = \"fs-meta shared library\""));
}

// @verify_spec("CONTRACTS.DOMAIN_IDENTITY.META_INDEX_DOMAIN_OWNERSHIP", mode="system")
#[test]
fn test_meta_index_domain_ownership_contract() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("META_INDEX_DOMAIN_OWNERSHIP"));

    let l2 = read_fs_meta_spec_file("fs-meta/specs/L2-ARCHITECTURE.md");
    assert!(l2.contains("Meta-Index Service (App Internal)"));
    assert!(l2.contains("not kernel-owned state"));

    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKFLOWS.md");
    assert!(
        l3.contains("fs-meta app 从 `meta-index` 读取物化 observation/projection 结果")
            || l3.contains("fs-meta app 从 `meta-index` 读取物化结果")
    );

    let app_lib = read_fs_meta_spec_file("fs-meta/app/src/lib.rs");
    assert!(app_lib.contains("mod sink;"));
    assert!(app_lib.contains("mod source;"));
}

// @verify_spec("CONTRACTS.KERNEL_RELATION.KERNEL_DOMAIN_NEUTRAL_CONSUMPTION", mode="system")
#[test]
fn test_kernel_domain_neutral_consumption_contract() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("KERNEL_DOMAIN_NEUTRAL_CONSUMPTION"));

    let l2 = read_fs_meta_spec_file("fs-meta/specs/L2-ARCHITECTURE.md");
    assert!(
        l2.contains("kernel generic route/channel calls")
            || l2.contains("kernel generic route lookup calls")
            || l2.contains("kernel generic route lookup/route calls")
    );

    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKFLOWS.md");
    assert!(
        l3.contains("通用 channel attach / query request")
            || l3.contains("通用 route call 请求")
            || l3.contains("通用 route call/route")
    );

    let runtime_seam = read_fs_meta_spec_file("fs-meta/app/src/runtime/seam.rs");
    let runtime_routes = read_fs_meta_spec_file("fs-meta/app/src/runtime/routes.rs");
    assert!(runtime_seam.contains("ChannelIoSubset"));
    assert!(runtime_seam.contains("ExchangeHostAdapter"));
    assert!(runtime_routes.contains("ROUTE_TOKEN_FS_META"));
    assert!(runtime_routes.contains("METHOD_QUERY"));
    assert!(runtime_routes.contains("METHOD_FIND"));
}

// @verify_spec("CONTRACTS.KERNEL_RELATION.HOST_DESCRIPTOR_GROUPING_IS_DOMAIN_POLICY", mode="system")
#[test]
fn test_host_descriptor_grouping_is_domain_policy() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("HOST_DESCRIPTOR_GROUPING_IS_DOMAIN_POLICY"));
    let l2 = read_fs_meta_spec_file("fs-meta/specs/L2-ARCHITECTURE.md");
    assert!(l2.contains("descriptor selectors"));
    assert!(l2.contains("host object grants"));
}

// @verify_spec("CONTRACTS.KERNEL_RELATION.APP_DEFINED_GROUPS_RUNTIME_BIND_RUN", mode="system")
#[test]
fn test_app_defined_groups_runtime_scheduled_contract() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("APP_DEFINED_GROUPS_RUNTIME_BIND_RUN"));

    let source_mod = read_fs_meta_spec_file("fs-meta/app/src/source/mod.rs");
    assert!(source_mod.contains("reconcile_root_tasks"));

    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKFLOWS.md");
    assert!(l3.contains("GroupFormationFromHostObjectDescriptorsWorkflow"));
    assert!(l3.contains("source 按版本号执行在线增量重收敛"));
}

// @verify_spec("CONTRACTS.KERNEL_RELATION.SOURCE_PRIMARY_EXECUTOR_APP_OWNED", mode="system")
#[test]
fn test_source_primary_executor_app_owned_contract() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("SOURCE_PRIMARY_EXECUTOR_APP_OWNED"));

    let source_mod = read_fs_meta_spec_file("fs-meta/app/src/source/mod.rs");
    assert!(
        source_mod.contains("source-primary executor")
            || source_mod.contains("source_primary")
            || source_mod.contains("is_group_primary")
    );
    assert!(source_mod.contains("root_task_signature") || source_mod.contains("task signature"));
    assert!(
        source_mod.contains("let watch_manager = if root.spec.watch")
            || source_mod.contains("watch_manager")
    );
    assert!(source_mod.contains("watcher::start_watch_loop("));
    assert!(source_mod.contains("audit_interval"));
    assert!(
        source_mod.contains("lock_or_recover(&self.state_cell.roots")
            || source_mod.contains("group planner")
    );
}

// @verify_spec("CONTRACTS.KERNEL_RELATION.RESOURCE_BOUND_SOURCE_LOCAL_HOST_PROGRAMMING", mode="system")
#[test]
fn test_resource_bound_source_local_host_programming_contract() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    let l2 = read_fs_meta_spec_file("fs-meta/specs/L2-ARCHITECTURE.md");
    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKFLOWS.md");
    let glossary = read_fs_meta_spec_file("fs-meta/specs/L0-GLOSSARY.md");
    assert!(l1.contains("RESOURCE_BOUND_SOURCE_LOCAL_HOST_PROGRAMMING"));
    assert!(l2.contains("host-local adaptation on the bound host") || l2.contains("bound host"));
    assert!(
        l3.contains("binding宿主")
            || l3.contains("绑定宿主本地")
            || l3.contains("绑定宿主本地发出低层元数据/目录/watch 请求")
    );
    assert!(glossary.contains("Mount-Root Object") && glossary.contains("公共 host-fs facade"));
}

// @verify_spec("CONTRACTS.KERNEL_RELATION.HOST_ADAPTER_SDK_TRANSLATION_BOUNDARY", mode="system")
#[test]
fn test_host_adapter_sdk_translation_boundary() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("HOST_ADAPTER_SDK_TRANSLATION_BOUNDARY"));
    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKFLOWS.md");
    assert!(l3.contains("host-adapter-sdk"));
}

// @verify_spec("CONTRACTS.KERNEL_RELATION.HOST_ADAPTER_SDK_TRANSLATION_BOUNDARY", mode="system")
#[test]
fn test_source_host_fs_calls_are_abstracted_through_host_adapter_sdk() {
    let source_mod = read_fs_meta_spec_file("fs-meta/app/src/source/mod.rs");
    let scanner = read_fs_meta_spec_file("fs-meta/app/src/source/scanner.rs");
    let watcher = read_fs_meta_spec_file("fs-meta/app/src/source/watcher.rs");

    assert!(
        scanner.contains("HostFsMeta"),
        "scanner should consume host-adapter-fs-meta HostFsMeta trait"
    );
    assert!(
        watcher.contains("HostFsWatch"),
        "watcher should consume host-adapter-fs-meta HostFsWatch trait"
    );
    assert!(
        !scanner.contains("std::fs::"),
        "scanner must avoid direct std::fs calls"
    );
    assert!(
        !watcher.contains("std::fs::"),
        "watcher must avoid direct std::fs calls"
    );
    assert!(
        !watcher.contains("use inotify::"),
        "fs-meta watcher must not import inotify directly"
    );
    assert!(source_mod.contains("HostFsFacade"));
    assert!(
        !source_mod.contains("LocalHostFsMeta"),
        "source runtime should not branch on local host-fs backend construction directly"
    );
    assert!(
        !source_mod.contains("RoutedHostFsMeta::new("),
        "source runtime should not branch on routed host-fs backend construction directly"
    );
    assert!(
        !source_mod.contains("RoutedHostFsWatchProvider::new("),
        "source runtime should not branch on routed watch backend construction directly"
    );
    assert!(
        !source_mod.contains("rpc_target_member_id("),
        "source runtime must not parse target member id from app-visible routing metadata"
    );
}

// @verify_spec("CONTRACTS.API_BOUNDARY.EMPTY_ROOTS_VALID_DEPLOYED_STATE", mode="system")
#[tokio::test]
async fn test_empty_roots_valid_deployed_state_without_silent_secondary_path() {
    use capanix_app_fs_meta::{FSMetaApp, FSMetaConfig};

    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("EMPTY_ROOTS_VALID_DEPLOYED_STATE"));
    let cfg = FSMetaConfig::from_manifest_config(&std::collections::HashMap::from([(
        "workers".to_string(),
        ConfigValue::Map(std::collections::HashMap::from([
            (
                "facade".to_string(),
                ConfigValue::Map(std::collections::HashMap::from([(
                    "mode".to_string(),
                    ConfigValue::String("embedded".to_string()),
                )])),
            ),
            (
                "source".to_string(),
                ConfigValue::Map(std::collections::HashMap::from([(
                    "mode".to_string(),
                    ConfigValue::String("embedded".to_string()),
                )])),
            ),
            (
                "scan".to_string(),
                ConfigValue::Map(std::collections::HashMap::from([(
                    "mode".to_string(),
                    ConfigValue::String("embedded".to_string()),
                )])),
            ),
            (
                "sink".to_string(),
                ConfigValue::Map(std::collections::HashMap::from([(
                    "mode".to_string(),
                    ConfigValue::String("embedded".to_string()),
                )])),
            ),
        ])),
    )]))
    .expect("in-process fs-meta config");
    let app = FSMetaApp::new(cfg, NodeId("node-a".into()))
        .expect("empty roots should be accepted as valid deployed state");
    drop(app);
}

// @verify_spec("CONTRACTS.INDEX_LIFECYCLE.REALTIME_PLUS_BASELINE_PLUS_AUDIT_PLUS_SENTINEL", mode="system")
#[test]
fn test_sentinel_actions_are_bridged_to_runtime_rescan_and_health_projection() {
    let source_mod = read_fs_meta_spec_file("fs-meta/app/src/source/mod.rs");
    let sentinel = read_fs_meta_spec_file("fs-meta/app/src/source/sentinel.rs");
    assert!(
        sentinel.contains("TriggerRescan"),
        "sentinel contract requires explicit rescan action"
    );
    assert!(
        source_mod.contains("tx.send(RescanReason::Manual)"),
        "sentinel trigger_rescan must bridge into source rescan path"
    );
    assert!(
        source_mod.contains("degraded: {reason}"),
        "sentinel degraded action should project explicit degraded state"
    );
}

// @verify_spec("CONTRACTS.INDEX_LIFECYCLE.REALTIME_PLUS_BASELINE_PLUS_AUDIT_PLUS_SENTINEL", mode="system")
#[test]
fn test_unified_audit_scan_and_drift_timestamp_contracts_are_migrated_to_main_specs() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("`1s`..`300s`"));
    assert!(l1.contains("watch-before-read"));
    assert!(l1.contains("executed only on group-primary members"));
    assert!(l1.contains("scan_workers"));
    assert!(l1.contains("batch_size"));
    assert!(l1.contains("audit_skipped=true"));
    assert!(l1.contains("non-primary members keep realtime watch/listen hot set"));
    assert!(l1.contains("visited directory identity `(dev, ino)`"));
    assert!(l1.contains("`IN_IGNORED` invalidation MUST purge wd/path"));
    assert!(l1.contains("`IN_Q_OVERFLOW`"));
    assert!(l1.contains("WatchOverflowPendingAudit"));
    assert!(l1.contains("MUST NOT trigger immediate full rescan"));
    assert!(l1.contains("shadow_now_us"));
    assert!(l1.contains("error-marker events are excluded"));

    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKFLOWS.md");
    assert!(l3.contains("UnifiedAuditScanWorkflow"));
    assert!(l3.contains("每个已调度 mount-root object 启动独立 watch 管线"));
    assert!(l3.contains("仅在各 group 的 source-primary executor 上执行 epoch0 初始基线扫描"));
    assert!(l3.contains("`watch=true` 目录执行 `watch-before-read`"));
    assert!(l3.contains("根目录自身的目录元数据记录"));
    assert!(l3.contains("audit_skipped=true"));
    assert!(l3.contains("`1s→2s→...→300s`"));
    assert!(l3.contains("目录身份集合 `(dev, ino)`"));
    assert!(l3.contains("`IN_Q_OVERFLOW`"));
    assert!(l3.contains("`ControlEvent::WatchOverflow`"));
    assert!(l3.contains("`IN_IGNORED`"));
    assert!(l3.contains("force-find 诊断错误标记事件"));
    assert!(l3.contains("不触发即时全盘补扫"));
    assert!(l3.contains("下一次 primary audit `EpochEnd` 后清除"));

    let scanner = read_fs_meta_spec_file("fs-meta/app/src/source/scanner.rs");
    assert!(scanner.contains("(0..self.scan_workers)"));
    assert!(scanner.contains("if current_batch.len() >= self.batch_size"));
    assert!(scanner.contains("Emit directory record (includes root on initial scan)."));
    assert!(scanner.contains("HashSet::<(u64, u64)>::new()"));
    assert!(scanner.contains("Symlink loop detected"));
    assert!(scanner.contains("let cached = cache.get(&dir_path).copied();"));
    assert!(scanner.contains("let mtime_unchanged ="));
    assert!(scanner.contains("FileMetaRecord::scan_update("));
    assert!(scanner.contains("!should_deep_scan"));

    let watcher = read_fs_meta_spec_file("fs-meta/app/src/source/watcher.rs");
    assert!(watcher.contains("HostFsWatchMask::Q_OVERFLOW"));
    assert!(watcher.contains("ControlEvent::WatchOverflow"));
    assert!(watcher.contains("RescanReason::Overflow"));
    assert!(watcher.contains("handle_in_ignored"));
    assert!(watcher.contains("Root directory deleted or unmounted"));
    assert!(watcher.contains("timestamp_us: drift::shadow_now_us(drift_us)"));

    let source_mod = read_fs_meta_spec_file("fs-meta/app/src/source/mod.rs");
    assert!(source_mod.contains("let mut backoff = Duration::from_secs(1);"));
    assert!(source_mod.contains("let max_backoff = Duration::from_secs(300);"));
    assert!(source_mod.contains("Root {} unavailable"));
    assert!(
        source_mod.contains("if root.spec.scan && root.is_group_primary")
            || source_mod.contains(
                "if root.spec.scan && Self::root_current_is_group_primary(&roots_handle, &root_key)"
            )
    );
    assert!(source_mod.contains("if root.is_group_primary"));
    assert!(source_mod.contains("RescanReason::Overflow"));
    assert!(source_mod.contains("HealthSignal::WatchOverflow"));
    assert!(source_mod.contains("timestamp_us: now_us()"));
    assert!(source_mod.contains("logical_ts: None"));

    let sink_mod = read_fs_meta_spec_file("fs-meta/app/src/sink/mod.rs");
    assert!(sink_mod.contains("overflow_pending_audit"));
    assert!(sink_mod.contains("ControlEvent::WatchOverflow"));

    assert!(l1.contains("WatchOverflowPendingAudit"));
}

// @verify_spec("CONTRACTS.INDEX_LIFECYCLE.GROUP_PARTITIONED_SINK_STATE", mode="system")
#[test]
fn test_sink_type_mutation_contract_is_migrated_to_main_specs() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("changes type (`dir<->file`)"));
    assert!(l1.contains("purges cached descendants"));

    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKFLOWS.md");
    assert!(l3.contains("类型变更（`dir<->file`）"));
    assert!(l3.contains("先清理该路径下已缓存的后代节点"));

    let arbitrator = read_fs_meta_spec_file("fs-meta/app/src/sink/arbitrator.rs");
    assert!(arbitrator.contains("existing.is_dir != record.unix_stat.is_dir"));
    assert!(arbitrator.contains("tree.purge_descendants(path);"));
}

// @verify_spec("CONTRACTS.INDEX_LIFECYCLE.GROUP_PARTITIONED_SINK_STATE", mode="system")
#[test]
fn test_sink_integrity_clock_contracts_are_migrated_to_main_specs() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("suspect-age evaluation uses shadow clock domain"));
    assert!(l1.contains("Scan with unchanged `mtime` preserves existing `monitoring_attested`"));
    assert!(l1.contains("cold start (`shadow_time_high_us=0`)"));
    assert!(
        l1.contains("extreme future payload `modified_time_us` MUST NOT advance sink shadow clock")
    );

    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKFLOWS.md");
    assert!(l3.contains("suspect 年龄判定仅使用 shadow-time 轴"));
    assert!(l3.contains("不得降级既有 attestation"));
    assert!(l3.contains("冷启动时 `shadow_time_high_us=0`"));
    assert!(l3.contains("drift `P999` 样本过滤"));

    let arbitrator = read_fs_meta_spec_file("fs-meta/app/src/sink/arbitrator.rs");
    assert!(arbitrator.contains("let age_secs = clock.now_secs() - mtime_secs;"));
    assert!(arbitrator.contains("if mtime_changed"));

    let clock = read_fs_meta_spec_file("fs-meta/app/src/sink/clock.rs");
    assert!(clock.contains("shadow_time_high_us: 0"));
    assert!(clock.contains("if timestamp_us > self.shadow_time_high_us"));

    let drift = read_fs_meta_spec_file("fs-meta/app/src/source/drift.rs");
    assert!(drift.contains("P999: 99.9th percentile"));
    assert!(drift.contains("if self.is_graduated()"));
}

// @verify_spec("CONTRACTS.INDEX_LIFECYCLE.REALTIME_PLUS_BASELINE_PLUS_AUDIT_PLUS_SENTINEL", mode="system")
#[test]
fn test_source_epoch_and_parent_context_contracts_are_migrated_to_main_specs() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("emit in-band `ControlEvent::EpochStart/EpochEnd`"));
    assert!(l1.contains("include `parent_path` and `parent_mtime_us` context"));

    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKFLOWS.md");
    assert!(l3.contains("`EpochStart(Audit, epoch_id)` 与 `EpochEnd(Audit, epoch_id)`"));
    assert!(l3.contains("`FileMetaRecord` 必须携带 `parent_path` 与 `parent_mtime_us`"));

    let scanner = read_fs_meta_spec_file("fs-meta/app/src/source/scanner.rs");
    assert!(scanner.contains("ControlEvent::EpochStart"));
    assert!(scanner.contains("ControlEvent::EpochEnd"));
    assert!(scanner.contains("parent_path"));
    assert!(scanner.contains("parent_mtime_us"));

    let sink_epoch = read_fs_meta_spec_file("fs-meta/app/src/sink/epoch.rs");
    assert!(sink_epoch.contains("process_control_event"));
    assert!(sink_epoch.contains("ControlEvent::EpochStart"));
    assert!(sink_epoch.contains("ControlEvent::EpochEnd"));
}

// @verify_spec("CONTRACTS.INDEX_LIFECYCLE.REALTIME_PLUS_BASELINE_PLUS_AUDIT_PLUS_SENTINEL", mode="system")
#[test]
fn test_source_wire_format_contracts_are_migrated_to_main_specs() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("payload serialization follows shared schema MessagePack contract"));
    assert!(l1.contains("leading-slash relative form (`/` for root path)"));

    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKFLOWS.md");
    assert!(l3.contains("输出 MessagePack `FileMetaRecord`"));
    assert!(l3.contains("`source` 字段按产出轨道显式标记 `Realtime/Scan`"));

    assert!(l1.contains("shared schema MessagePack contract"));
    assert!(l1.contains("leading-slash relative form"));

    let watcher = read_fs_meta_spec_file("fs-meta/app/src/source/watcher.rs");
    assert!(watcher.contains("return b\"/\".to_vec();"));
    assert!(watcher.contains("buf.push(b'/');"));
}

// @verify_spec("CONTRACTS.INDEX_LIFECYCLE.REALTIME_PLUS_BASELINE_PLUS_AUDIT_PLUS_SENTINEL", mode="system")
#[test]
fn test_source_watch_before_read_and_always_recurse_contracts_are_migrated_to_main_specs() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("watch registration unified (`watch-before-read`)"));
    assert!(l1.contains("not realtime-covered (`watch=false`"));

    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKFLOWS.md");
    assert!(l3.contains("`watch=true` 目录执行 `watch-before-read`"));
    assert!(l3.contains("命中静默目录时跳过文件级 stat"));
    assert!(l3.contains("仍递归子目录"));

    let scanner = read_fs_meta_spec_file("fs-meta/app/src/source/scanner.rs");
    assert!(scanner.contains("mgr.schedule(&dir_path)"));
    assert!(scanner.contains("ALWAYS_RECURSE: still descend into subdirectories"));
}

// @verify_spec("CONTRACTS.INDEX_LIFECYCLE.REALTIME_PLUS_BASELINE_PLUS_AUDIT_PLUS_SENTINEL", mode="system")
#[test]
fn test_source_error_isolation_and_attrib_watch_contracts_are_migrated_to_main_specs() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains(
        "drift sample collection failure or empty sample window MUST NOT block `pub_()` startup"
    ));
    assert!(l1.contains("file-level transient races (`ENOENT`)"));
    assert!(l1.contains("attribute-only watch events (`IN_ATTRIB`"));

    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKFLOWS.md");
    assert!(l3.contains("drift 基线保持 `0`"));
    assert!(l3.contains("单条目错误（如 `ENOENT`"));
    assert!(l3.contains("`IN_ATTRIB`；该类事件按 `Update + is_atomic_write=false` 发出"));

    let drift = read_fs_meta_spec_file("fs-meta/app/src/source/drift.rs");
    assert!(drift.contains("current_drift_us: 0"));

    let scanner = read_fs_meta_spec_file("fs-meta/app/src/source/scanner.rs");
    assert!(scanner.contains("Skipping path {:?}: metadata failed"));
    assert!(scanner.contains("Skipping directory {:?}: read_dir failed"));
    assert!(scanner.contains("scanner.parallel_walk.push_scan_drift"));

    let watcher = read_fs_meta_spec_file("fs-meta/app/src/source/watcher.rs");
    assert!(watcher.contains("HostFsWatchMask::ATTRIB"));
    assert!(watcher.contains("is_atomic: ATTRIB ≠ data stabilization"));
    assert!(watcher.contains("Skipping path {:?}: metadata failed"));
    assert!(watcher.contains("drift sample skipped for {:?}: metadata failed"));
    assert!(watcher.contains("drift sample skipped for {:?}: modified() missing"));
}

// @verify_spec("CONTRACTS.INDEX_LIFECYCLE.REALTIME_PLUS_BASELINE_PLUS_AUDIT_PLUS_SENTINEL", mode="system")
#[cfg(not(target_os = "linux"))]
#[tokio::test]
async fn test_non_linux_strict_fail_for_realtime_startup() {
    use capanix_app_fs_meta::{FSMetaApp, FSMetaConfig};
    let cfg_map = HashMap::from([(
        "roots".to_string(),
        ConfigValue::Array(vec![ConfigValue::Map(HashMap::from([(
            "path".to_string(),
            ConfigValue::String("/tmp".to_string()),
        )]))]),
    )]);
    let cfg = FSMetaConfig::from_manifest_config(&cfg_map).expect("valid root config");
    let app = FSMetaApp::new(cfg, NodeId("node-a".into())).expect("init fs-meta app");
    let err = app
        .start()
        .await
        .expect_err("non-linux platform must fail-fast");
    assert!(matches!(err, CnxError::NotSupported(_)));
}

// @verify_spec("CONTRACTS.QUERY_OUTCOME.QUERY_PATH_OUTPUT_DISCIPLINE", mode="system")
#[test]
fn test_unified_query_shape_output() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("QUERY_PATH_OUTPUT_DISCIPLINE"));
    assert!(l1.contains("grouped envelope"));
    assert!(l1.contains("top-level `path/status/group_order/groups/group_page`"));
    assert!(l1.contains("optional `root/entries/entry_page`"));
}

// @verify_spec("CONTRACTS.QUERY_OUTCOME.GROUP_ORDER_MULTI_GROUP_BUCKET_SELECTION", mode="system")
#[test]
fn test_group_order_multi_group_bucket_selection() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("GROUP_ORDER_MULTI_GROUP_BUCKET_SELECTION"));
    assert!(l1.contains("group_order"));
    assert!(l1.contains("group_page_size=1"));
    assert!(l1.contains("file-count"));
    assert!(l1.contains("file-age"));
    assert!(l1.contains("group-key"));
    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKFLOWS.md");
    assert!(l3.contains("GroupOrderedBucketQuery"));
    assert!(l3.contains("group_order"));
    assert!(l3.contains("group_page_size"));
    assert!(l3.contains("file-count"));
    assert!(l3.contains("file-age"));
    assert!(l3.contains("lightweight phase-1 `stats` probe"));
    assert!(l3.contains("group_page_size=1"));
}

// @verify_spec("CONTRACTS.INDEX_LIFECYCLE.DELETE_SEMANTICS_PRESERVATION", mode="system")
#[test]
fn test_delete_semantics_preservation() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("DELETE_SEMANTICS_PRESERVATION"));
    assert!(l1.contains("preserve delete semantics"));
}

// @verify_spec("CONTRACTS.INDEX_LIFECYCLE.DELETE_SEMANTICS_SPLIT", mode="system")
#[test]
fn test_delete_semantics_split_contract() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("DELETE_SEMANTICS_SPLIT"));
    assert!(l1.contains("sink_tombstone_ttl_ms"));
    assert!(l1.contains("sink_tombstone_tolerance_us"));
    assert!(l1.contains("short-lived false-positive reappearance"));

    let arbitrator = read_fs_meta_spec_file("fs-meta/app/src/sink/arbitrator.rs");
    assert!(arbitrator.contains("Authoritative delete — create inline tombstone"));
    assert!(arbitrator.contains("Hard remove (not tombstone) for Scan deletes"));

    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKFLOWS.md");
    assert!(l3.contains("Realtime delete 保持 tombstone 语义"));
    assert!(l3.contains("MID 不创建 tombstone"));
}

// @verify_spec("CONTRACTS.QUERY_OUTCOME.EXPLICIT_QUERY_STATUS_AND_METADATA_AVAILABILITY", mode="system")
#[test]
fn test_partial_failure_member_envelope() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("EXPLICIT_QUERY_STATUS_AND_METADATA_AVAILABILITY"));
    assert!(l1.contains("`ok`/`error`"));
    assert!(l1.contains("withheld_reason"));
}

// @verify_spec("CONTRACTS.QUERY_OUTCOME.QUERY_HTTP_FACADE_DEFAULTS_AND_SHAPE", mode="system")
// @verify_spec("CONTRACTS.QUERY_OUTCOME.ORIGIN_TRACE_DRIVEN_GROUP_AGGREGATION", mode="system")
// @verify_spec("CONTRACTS.QUERY_OUTCOME.GROUPED_QUERY_PARTIAL_FAILURE_HTTP_SUCCESS_ENVELOPE", mode="system")
// @verify_spec("CONTRACTS.QUERY_OUTCOME.QUERY_TRANSPORT_TIMEOUT_AND_ERROR_MAPPING", mode="system")
// @verify_spec("CONTRACTS.QUERY_OUTCOME.QUERY_BOUND_ROUTE_METRICS_DIAGNOSTICS_BOUNDARY", mode="system")
// @verify_spec("CONTRACTS.QUERY_OUTCOME.QUERY_CALLER_FLOW_LIFECYCLE_IMPLEMENTATION_DEFINED", mode="system")
#[test]
fn test_projection_view_contracts_are_migrated_to_main_specs() {
    let l0 = read_fs_meta_spec_file("fs-meta/specs/L0-VISION.md");
    assert!(l0.contains("QUERY_HTTP_FACADE"));
    assert!(l0.contains("STATELESS_QUERY_PROXY_AGGREGATION"));
    assert!(l0.contains("QUERY_TRANSPORT_DIAGNOSTICS"));

    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("QUERY_HTTP_FACADE_DEFAULTS_AND_SHAPE"));
    assert!(l1.contains("ORIGIN_TRACE_DRIVEN_GROUP_AGGREGATION"));
    assert!(l1.contains("GROUPED_QUERY_PARTIAL_FAILURE_HTTP_SUCCESS_ENVELOPE"));
    assert!(l1.contains("QUERY_TRANSPORT_TIMEOUT_AND_ERROR_MAPPING"));
    assert!(l1.contains("QUERY_BOUND_ROUTE_METRICS_DIAGNOSTICS_BOUNDARY"));
    assert!(l1.contains("QUERY_CALLER_FLOW_LIFECYCLE_IMPLEMENTATION_DEFINED"));

    let l3_workflows = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKFLOWS.md");
    assert!(l3_workflows.contains("ProjectionHttpQueryFacade"));
    assert!(l3_workflows.contains("origin_id"));
    assert!(l3_workflows.contains("/api/fs-meta/v1/bound-route-metrics"));
    assert!(l3_workflows.contains("transport/protocol/timeout 异常返回结构化错误体"));
    assert!(l3_workflows.contains("调用方通道生命周期与 correctness 解耦"));

    let l3_http = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/API_HTTP.md");
    assert!(l3_http.contains("ProjectionQueryEndpoints"));
    assert!(l3_http.contains("\"group_order\": GroupOrder"));
    assert!(l3_http.contains("\"pit\": PitHandle"));
    assert!(l3_http.contains("\"groups\": TreeGroupEnvelope[]"));
    assert!(l3_http.contains("\"group_page\": GroupPage"));
    assert!(l3_http.contains("GroupEnvelopeStats"));

    let projection_api = read_fs_meta_spec_file("fs-meta/app/src/query/api.rs");
    assert!(projection_api.contains(".route(\"/stats\""));
    assert!(projection_api.contains(".route(\"/tree\""));
    assert!(projection_api.contains(".route(\"/on-demand-force-find\""));
    assert!(projection_api.contains(".route(\"/bound-route-metrics\""));
    assert!(projection_api.contains("QUERY_TIMEOUT_MS_DEFAULT: u64 = 30_000"));
    assert!(projection_api.contains("FORCE_FIND_TIMEOUT_MS_DEFAULT: u64 = 60_000"));
    assert!(projection_api.contains("normalize_api_params"));
    assert!(projection_api.contains("body.insert(\"group_order\""));
    assert!(projection_api.contains("\"pit\".into()"));
    assert!(projection_api.contains("\"group_page\".into()"));
}

// @verify_spec("CONTRACTS.FAILURE_ISOLATION_BOUNDARY.INTERFACE_TASK_OR_WORKER_FAILURE_CONTAINMENT_TARGET", mode="system")
// @verify_spec("CONTRACTS.FAILURE_ISOLATION_BOUNDARY.EXECUTION_FAILURE_DOMAINS_ARE_EXPLICIT", mode="system")
#[test]
fn test_interface_endpoints_use_managed_task_lifecycle_and_bounded_shutdown() {
    let l0 = read_fs_meta_spec_file("fs-meta/specs/L0-VISION.md");
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    let source = read_fs_meta_spec_file("fs-meta/app/src/source/mod.rs");
    let sink = read_fs_meta_spec_file("fs-meta/app/src/sink/mod.rs");
    let endpoint_runtime = read_fs_meta_spec_file("fs-meta/app/src/runtime/endpoint.rs");

    assert!(l0.contains("EXPLICIT_EXECUTION_FAILURE_DOMAINS"));
    assert!(l0.contains("TASK_OR_WORKER_FAILURE_CONTAINMENT"));
    assert!(l0.contains("FAILURE_IMPACT_DECLARED_BY_MODE"));
    assert!(l1.contains("Covers L0: VISION.EXPLICIT_EXECUTION_FAILURE_DOMAINS"));
    assert!(l1.contains("Covers L0: VISION.TASK_OR_WORKER_FAILURE_CONTAINMENT"));
    assert!(l1.contains("Covers L0: VISION.FAILURE_IMPACT_DECLARED_BY_MODE"));
    assert!(source.contains("ManagedEndpointTask"));
    assert!(sink.contains("ManagedEndpointTask"));
    assert!(source.contains("task.shutdown(Duration::from_secs(2))"));
    assert!(sink.contains("task.shutdown(Duration::from_secs(2))"));
    assert!(endpoint_runtime.contains("CancellationToken"));
}

// @verify_spec("CONTRACTS.FAILURE_ISOLATION_BOUNDARY.FAILURE_IMPACT_BY_EXECUTION_SHAPE_DECLARED", mode="system")
#[test]
fn test_process_level_failure_impact_declared_contract() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("FAILURE_IMPACT_BY_EXECUTION_SHAPE_DECLARED"));
    assert!(l1.contains("in-process crash semantics"));
    assert!(l1.contains("runtime lifecycle restart/rebind and rebuild paths"));

    let l2 = read_fs_meta_spec_file("fs-meta/specs/L2-ARCHITECTURE.md");
    assert!(l2.contains("Single fs-meta App Boundary"));
    assert!(l2.contains("one app package boundary"));
}

// @verify_spec("CONTRACTS.QUERY_OUTCOME.CORRELATION_ID_CONTINUITY", mode="system")
#[test]
fn test_correlation_id_continuity() {
    let source = read_fs_meta_spec_file("fs-meta/app/src/source/mod.rs");
    let sink = read_fs_meta_spec_file("fs-meta/app/src/sink/mod.rs");
    assert!(source.contains("meta.correlation_id"));
    assert!(source.contains("req.metadata().correlation_id"));
    assert!(sink.contains("meta.correlation_id"));
    assert!(sink.contains("req.metadata().correlation_id"));
}

// @verify_spec("CONTRACTS.INDEX_LIFECYCLE.IN_MEMORY_MATERIALIZED_INDEX_BASELINE", mode="system")
// @verify_spec("CONTRACTS.INDEX_LIFECYCLE.GROUP_PARTITIONED_SINK_STATE", mode="system")
// @verify_spec("CONTRACTS.INDEX_LIFECYCLE.AUTHORITATIVE_JOURNAL_TRUTH_LEDGER", mode="system")
#[test]
fn test_in_memory_materialized_index_baseline_contract() {
    let l0 = read_fs_meta_spec_file("fs-meta/specs/L0-VISION.md");
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("IN_MEMORY_MATERIALIZED_INDEX_BASELINE"));
    assert!(l1.contains("AUTHORITATIVE_JOURNAL_TRUTH_LEDGER"));
    assert!(l1.contains("Covers L0: VISION.SINK_SINGLE_TREE_ARBITRATION"));
    assert!(l0.contains("AUTHORITATIVE_TRUTH_LEDGER"));
    assert!(l1.contains("SourceStateCell"));
    assert!(l1.contains("startup MUST fail closed"));
    assert!(l1.contains("state_class"));
    assert!(l1.contains("statecell_read/write/watch"));
    assert!(l1.contains("OPTIONAL_STATE_CARRIER_RUNTIME_HOSTING"));
    assert!(l1.contains("statecell_retire_binding"));
    assert!(l1.contains("runtime-owned carrier lifecycle seams"));
    assert!(l1.contains("domain truth ledger"));
    assert!(!l1.contains("statecell_authority_journal_dir"));
    assert!(l1.contains("removed authority carrier fields"));
    assert!(!l1.contains("sink_execution_mode"));
    assert!(!l1.contains("auto|memory|jsonl|external"));

    let l2 = read_fs_meta_spec_file("fs-meta/specs/L2-ARCHITECTURE.md");
    assert!(
        l2.contains("fs-meta authority journal remains the domain authoritative truth ledger")
            || l2.contains("authoritative truth ledger")
    );
    assert!(l2.contains("projection/materialized state remains rebuildable observation state"));
    assert!(l2.contains("Execution hosting choices for sink remain internal execution carriers"));
    assert!(!l2.contains("SourceStateCell"));
    assert!(!l2.contains("SinkStateCell"));
    assert!(!l2.contains("statecell_*"));
    assert!(!l2.contains("sink_execution_mode"));
    assert!(!l2.contains("state_class"));
    assert!(!l2.contains("unit_authority_state_dir"));
    assert!(!l2.contains("unit_authority_state_carrier"));

    let tree = read_fs_meta_spec_file("fs-meta/app/src/sink/tree.rs");
    assert!(
        tree.contains("Query-ready materialized tree")
            || tree.contains("query-ready exact-path")
            || tree.contains("query-ready exact-path arena"),
        "sink tree should remain an in-memory query-ready baseline"
    );
    assert!(
        tree.contains("pub struct MaterializedTree"),
        "sink tree should keep an explicit in-memory materialized tree type"
    );
    assert!(
        tree.contains("DirAggregate") || tree.contains("per-directory aggregate"),
        "sink tree should expose per-directory aggregate support"
    );

    let sink_mod = read_fs_meta_spec_file("fs-meta/app/src/sink/mod.rs");
    assert!(sink_mod.contains("pub(crate) groups: BTreeMap<String, GroupSinkState>"));
    assert!(sink_mod.contains("pub(crate) groups: BTreeMap<String, GroupSinkState>"));
    assert!(sink_mod.contains("struct SinkStateCell"));
    assert!(sink_mod.contains("commit_boundary: CommitBoundary"));
    assert!(sink_mod.contains("record_authoritative_commit"));
    assert!(sink_mod.contains("SINK_RUNTIME_UNIT_ID"));
    assert!(sink_mod.contains("AuthorityJournal::from_state_boundary("));
    assert!(
        sink_mod.contains("local_state_boundary_bridge")
            || sink_mod.contains("AuthorityJournal::from_state_boundary(")
    );
    assert!(!sink_mod.contains("embedded_state_boundary_bridge"));

    let source_mod = read_fs_meta_spec_file("fs-meta/app/src/source/mod.rs");
    assert!(source_mod.contains("struct SourceStateCell"));
    assert!(source_mod.contains("state_cell: SourceStateCell"));
    assert!(source_mod.contains("SourceStateCell::new("));
    assert!(source_mod.contains("commit_boundary: CommitBoundary"));
    assert!(source_mod.contains("record_authoritative_commit"));
    assert!(!source_mod.contains("statecell_authority_journal_dir"));
    assert!(!source_mod.contains("unit_authority_state_dir"));
    assert!(!source_mod.contains("unit_authority_state_carrier"));
    assert!(source_mod.contains("SOURCE_RUNTIME_UNIT_ID"));
    assert!(source_mod.contains("AuthorityJournal::from_state_boundary("));
    assert!(
        source_mod.contains("local_state_boundary_bridge")
            || source_mod.contains("AuthorityJournal::from_state_boundary(")
    );
    assert!(!source_mod.contains("embedded_state_boundary_bridge"));

    let commit_boundary_mod = read_fs_meta_spec_file("fs-meta/app/src/state/commit_boundary.rs");
    assert!(commit_boundary_mod.contains("struct CommitBoundary"));
    assert!(commit_boundary_mod.contains("Unified side-effect commit boundary"));

    let state_cell_mod = read_fs_meta_spec_file("fs-meta/app/src/state/cell.rs");
    assert!(state_cell_mod.contains("struct AuthorityJournal"));
    assert!(state_cell_mod.contains("from_state_boundary"));
    assert!(state_cell_mod.contains("StateClass::Authoritative"));
    assert!(state_cell_mod.contains("local_state_boundary_bridge"));
    assert!(!state_cell_mod.contains("embedded_state_plane_bridge"));

    let cargo = read_fs_meta_spec_file("fs-meta/app/Cargo.toml");
    assert!(
        !cargo.contains("rocksdb") && !cargo.contains("sled") && !cargo.contains("sqlite"),
        "baseline should not require durable index backend dependency"
    );
}

// @verify_spec("CONTRACTS.EVOLUTION_AND_OPERATIONS.API_FAILOVER_RESCAN_REBUILD", mode="system")
#[test]
fn test_api_failover_rescan_rebuild_contract() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("API_FAILOVER_RESCAN_REBUILD"));

    let l2 = read_fs_meta_spec_file("fs-meta/specs/L2-ARCHITECTURE.md");
    assert!(l2.contains("resource-scoped HTTP facade semantics"));

    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKFLOWS.md");
    assert!(l3.contains("进程重启或 owner 切换后必须通过扫描/审计链路重建"));
    assert!(l3.contains("扫描/审计链路重建"));
}

// @verify_spec("CONTRACTS.EVOLUTION_AND_OPERATIONS.GROUP_AWARE_SINK_BIND_RUN", mode="system")
#[test]
fn test_group_aware_sink_scheduling_contract() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("GROUP_AWARE_SINK_BIND_RUN"));

    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKFLOWS.md");
    assert!(l3.contains("GroupAwareSinkBindRun"));
    assert!(l3.contains("runtime 返回每个 sink instance"));
    assert!(l3.contains("bound_scopes[]"));

    let cfg = read_fs_meta_spec_file("fs-meta/testdata/specs/fs-meta-contract-tests.config.md");
    assert!(cfg.contains("group-aware bind/run 由 runtime 执行"));
    assert!(cfg.contains("不内建 fs-meta 组语义或 primary 规则"));
}

// @verify_spec("CONTRACTS.EVOLUTION_AND_OPERATIONS.UNIT_CONTROL_ENVELOPE_FENCING", mode="system")
#[test]
fn test_unit_control_envelope_fencing_contract() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("UNIT_CONTROL_ENVELOPE_FENCING"));
    assert!(l1.contains("unit_id") || l1.contains("worker_id"));
    assert!(l1.contains("generation"));
    assert!(l1.contains("runtime.exec.*"));
    assert!(l1.contains("runtime.exec.*"));
    assert!(l1.contains("source/sink unit gate"));

    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKFLOWS.md");
    assert!(l3.contains("UnitControlFenceWorkflow"));
    assert!(l3.contains("未知 unit"));
    assert!(l3.contains("generation"));

    let cfg = read_fs_meta_spec_file("fs-meta/testdata/specs/fs-meta-contract-tests.config.md");
    assert!(cfg.contains("unit_id + generation"));
    assert!(cfg.contains("未知 unit 拒绝"));
    assert!(cfg.contains("过期代际信号忽略"));
    assert!(cfg.contains("runtime.exec.*"));

    let source = read_fs_meta_spec_file("fs-meta/app/src/source/mod.rs");
    assert!(source.contains("RuntimeUnitGate::new"));
    assert!(source.contains("SOURCE_RUNTIME_UNITS"));
    assert!(source.contains("unsupported unit_id"));
    assert!(source.contains("ignore stale activate"));
    assert!(source.contains("ignore stale deactivate"));
    assert!(source.contains("stale_deactivate_generation_is_ignored"));
    assert!(source.contains("stale_activate_generation_is_ignored"));

    let sink = read_fs_meta_spec_file("fs-meta/app/src/sink/mod.rs");
    assert!(sink.contains("RuntimeUnitGate::new"));
    assert!(sink.contains("SINK_RUNTIME_UNITS"));
    assert!(sink.contains("unsupported unit_id"));
    assert!(sink.contains("ignore stale activate"));
    assert!(sink.contains("ignore stale deactivate"));
    assert!(sink.contains("stale_deactivate_generation_is_ignored"));
    assert!(sink.contains("stale_activate_generation_is_ignored"));

    let gate = read_fs_meta_spec_file("fs-meta/app/src/runtime/unit_gate.rs");
    assert!(gate.contains("struct RuntimeUnitGate"));
    assert!(gate.contains("unsupported unit_id"));

    let units = read_fs_meta_spec_file("fs-meta/app/src/runtime/execution_units.rs");
    assert!(units.contains("SOURCE_RUNTIME_UNITS"));
    assert!(units.contains("SINK_RUNTIME_UNITS"));
    assert!(!units.contains("runtime.exec.unsupported"));

    for unit in [
        "runtime.exec.source",
        "runtime.exec.scan",
        "runtime.exec.sink",
    ] {
        assert!(
            units.contains(unit),
            "execution_units set must include {unit}"
        );
    }
}

// @verify_spec("CONTRACTS.API_BOUNDARY.QUERY_PATH_PARAMETERS_OWN_PAYLOAD_SHAPE", mode="system")
#[test]
fn test_query_limit_param_owns_payload_bound_contract() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("QUERY_PATH_PARAMETERS_OWN_PAYLOAD_SHAPE"));
    let l2 = read_fs_meta_spec_file("fs-meta/specs/L2-ARCHITECTURE.md");
    assert!(l2.contains("query/find payload shaping is modeled on query-path parameters"));
    let l3_api = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/API_HTTP.md");
    assert!(l3_api.contains("pit_id"));
    assert!(l3_api.contains("path_b64"));
    assert!(l3_api.contains("group_page_size` (default `64`, range `1..=1000`)"));
    assert!(l3_api.contains("entry_page_size` (default `1000`, range `1..=10000`)"));
    let l3_workflows = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKFLOWS.md");
    assert!(l3_workflows.contains("query-shaping 参数"));
    assert!(l3_workflows.contains("`path` 与 `path_b64` 二选一"));

    let api_types = read_fs_meta_spec_file("fs-meta/app/src/api/types.rs");
    let api_handlers = read_fs_meta_spec_file("fs-meta/app/src/api/handlers.rs");
    assert!(
        !api_types.contains("limit"),
        "management API types should not carry query/find payload limit body field"
    );
    assert!(
        !api_handlers.contains("limit"),
        "management API handlers should not implement query/find payload limit body field"
    );

    let projection_api = read_fs_meta_spec_file("fs-meta/app/src/query/api.rs");
    assert!(
        projection_api.contains("pub path_b64:"),
        "projection query params should expose path_b64 field"
    );
    assert!(
        projection_api.contains("pub pit_id:"),
        "projection query params should expose pit_id field"
    );
    assert!(
        projection_api.contains("pub group_page_size:"),
        "projection query params should expose group_page_size field"
    );
    assert!(
        projection_api.contains("pub entry_page_size:"),
        "projection query params should expose entry_page_size field"
    );
    assert!(
        projection_api.contains("normalize_group_page_size"),
        "projection query handlers should enforce group_page_size normalization"
    );
    assert!(
        projection_api.contains("normalize_entry_page_size"),
        "projection query handlers should enforce entry_page_size normalization"
    );
    assert!(
        projection_api.contains("decode_path_param"),
        "projection query handlers should normalize path/path_b64 into raw bytes"
    );
}

// @verify_spec("CONTRACTS.API_BOUNDARY.FS_META_HTTP_API_BOUNDARY", mode="system")
#[test]
fn test_fs_meta_http_api_boundary_contract() {
    let l0 = read_fs_meta_spec_file("fs-meta/specs/L0-VISION.md");
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/API_HTTP.md");
    assert!(l0.contains("BOUNDED_PRODUCT_MANAGEMENT_NAMESPACE"));
    assert!(l0.contains("SEPARATE_QUERY_AND_MANAGEMENT_AUTH_SUBJECTS"));
    assert!(l0.contains("LOCAL_AUTH_WITHOUT_PLATFORM_BYPASS"));
    assert!(l0.contains("ONLINE_SCOPE_MUTATION_AND_REPAIR"));
    assert!(l1.contains("FS_META_HTTP_API_BOUNDARY"));
    assert!(l1.contains("Covers L0: VISION.BOUNDED_PRODUCT_MANAGEMENT_NAMESPACE"));
    assert!(l1.contains("Covers L0: VISION.LOCAL_AUTH_WITHOUT_PLATFORM_BYPASS"));
    assert!(l1.contains("Covers L0: VISION.SEPARATE_QUERY_AND_MANAGEMENT_AUTH_SUBJECTS"));
    assert!(l1.contains("Covers L0: VISION.ONLINE_SCOPE_MUTATION_AND_REPAIR"));
    assert!(l3.contains("GET /status"));
    assert!(l3.contains("QueryApiKeyManagementEndpoints"));
    assert!(l3.contains("last_audit_completed_at_us"));
    assert!(l3.contains("groups[].initial_audit_completed"));
    assert!(l3.contains("groups[].overflow_pending_audit"));
    assert!(l3.contains("observation-evidence boundary"));
    assert!(l3.contains("optional facade-pending diagnostics"));
    assert!(l3.contains("`facade.pending`"));
    assert!(l3.contains("retry_attempts"));
    assert_domain_fs_meta_contract_test(
        "CONTRACTS.API_BOUNDARY.FS_META_HTTP_API_BOUNDARY",
        "test_fs_meta_http_api_boundary_contract",
    );
}

// @verify_spec("CONTRACTS.EVOLUTION_AND_OPERATIONS.GLOBAL_HTTP_API_RESOURCE_SCOPED_APP_FACADE", mode="system")
#[test]
fn test_global_http_api_resource_scoped_app_facade_contract() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    let l2 = read_fs_meta_spec_file("fs-meta/specs/L2-ARCHITECTURE.md");
    let api_server = read_fs_meta_spec_file("fs-meta/app/src/api/server.rs");
    assert!(l1.contains("GLOBAL_HTTP_API_RESOURCE_SCOPED_APP_FACADE"));
    assert!(l1.contains("resource-scoped one-cardinality facade"));
    assert!(l2.contains("Single fs-meta App Boundary"));
    assert!(l2.contains("resource-scoped HTTP facade semantics"));
    assert!(api_server.contains("/api/fs-meta/v1"));
}

// @verify_spec("CONTRACTS.API_BOUNDARY.MANUAL_RESCAN_OPERATION", mode="system")
#[test]
fn test_manual_rescan_operation_contract() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("MANUAL_RESCAN_OPERATION"));
    assert_domain_fs_meta_contract_test(
        "CONTRACTS.API_BOUNDARY.MANUAL_RESCAN_OPERATION",
        "test_manual_rescan_operation_contract",
    );
}

// @verify_spec("CONTRACTS.API_BOUNDARY.PRODUCT_API_NAMESPACE_STABILITY", mode="system")
#[test]
fn test_product_api_namespace_stability_contract() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/API_HTTP.md");
    let product_api = read_fs_meta_spec_file("fs-meta/app/src/product/mod.rs");
    assert!(l1.contains("PRODUCT_API_NAMESPACE_STABILITY"));
    assert!(l3.contains("Legacy product-management paths"));
    assert!(
        product_api.contains("pub use crate::api::types::RootEntry;")
            && product_api.contains(
                "pub use crate::api::{ApiAuthConfig, BootstrapAdminConfig, BootstrapManagementConfig};"
            )
            && product_api.contains("pub use release_doc::{FsMetaReleaseSpec, build_release_doc_value};"),
        "fs-meta app should expose a bounded product namespace for CLI/tooling consumers"
    );
    assert_domain_fs_meta_contract_test(
        "CONTRACTS.API_BOUNDARY.PRODUCT_API_NAMESPACE_STABILITY",
        "test_product_api_namespace_stability_contract",
    );
}

// @verify_spec("CONTRACTS.API_BOUNDARY.PRODUCT_CONSOLE_ACCESS_BOUNDARY", mode="system")
#[test]
fn test_product_console_roots_write_guard_contract() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/API_HTTP.md");
    let workflows = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKFLOWS.md");
    let server = read_fs_meta_spec_file("fs-meta/app/src/api/server.rs");
    assert!(l1.contains("PRODUCT_CONSOLE_ACCESS_BOUNDARY"));
    assert!(l3.contains("management session can access `/status`, `/runtime/grants`, `/monitoring/roots`, `/monitoring/roots/preview`, `PUT /monitoring/roots`, `POST /index/rescan`, and query-api-key management endpoints."));
    assert!(l3.contains("GET    /query-api-keys"));
    assert!(l3.contains("POST   /query-api-keys"));
    assert!(l3.contains("DELETE /query-api-keys/:key_id"));
    assert!(workflows.contains("QueryApiKeyLifecycle"));
    assert!(workflows.contains("GET /api/fs-meta/v1/query-api-keys"));
    assert!(workflows.contains("DELETE /api/fs-meta/v1/query-api-keys/:key_id"));
    assert!(server.contains("/api/fs-meta/v1/query-api-keys"));
    assert!(server.contains("/api/fs-meta/v1/query-api-keys/:key_id"));
    assert_domain_fs_meta_contract_test(
        "CONTRACTS.API_BOUNDARY.PRODUCT_CONSOLE_ACCESS_BOUNDARY",
        "test_product_console_roots_write_guard_contract",
    );
}

// @verify_spec("CONTRACTS.API_BOUNDARY.RUNTIME_GRANT_DISCOVERY_BOUNDARY", mode="system")
#[test]
fn test_runtime_grant_discovery_boundary_contract() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/API_HTTP.md");
    assert!(l1.contains("RUNTIME_GRANT_DISCOVERY_BOUNDARY"));
    assert!(l3.contains("GET /runtime/grants"));
    assert_domain_fs_meta_contract_test(
        "CONTRACTS.API_BOUNDARY.RUNTIME_GRANT_DISCOVERY_BOUNDARY",
        "test_runtime_grant_discovery_boundary_contract",
    );
}

// @verify_spec("CONTRACTS.API_BOUNDARY.UNIX_STYLE_LOCAL_AUTH_IN_DOMAIN", mode="system")
#[test]
fn test_unix_style_local_auth_in_domain_contract() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("UNIX_STYLE_LOCAL_AUTH_IN_DOMAIN"));
    assert_domain_fs_meta_contract_test(
        "CONTRACTS.API_BOUNDARY.UNIX_STYLE_LOCAL_AUTH_IN_DOMAIN",
        "test_unix_style_local_auth_in_domain_contract",
    );
}

// @verify_spec("CONTRACTS.API_BOUNDARY.ROLE_GROUP_ACCESS_GUARD", mode="system")
#[test]
fn test_role_group_access_guard_contract() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKFLOWS.md");
    let auth = read_fs_meta_spec_file("fs-meta/app/src/api/auth.rs");
    assert!(l1.contains("ROLE_GROUP_ACCESS_GUARD"));
    assert!(l1.contains("configured `management_group`"));
    assert!(l3.contains("configured `management_group`"));
    assert!(l3.contains("query-api-key authorization"));
    assert!(auth.contains("authorize_management_session("));
    assert!(auth.contains("self.cfg.management_group"));
    assert_domain_fs_meta_contract_test(
        "CONTRACTS.API_BOUNDARY.ROLE_GROUP_ACCESS_GUARD",
        "test_role_group_access_guard_contract",
    );
}

// @verify_spec("CONTRACTS.APP_SCOPE.OPAQUE_INTERNAL_PORTS_ONLY", mode="system")
// @verify_spec("CONTRACTS.APP_SCOPE.WORKER_MODE_FAILURE_BOUNDARY_IS_EXPLICIT", mode="system")
#[test]
fn test_runtime_support_transport_supervision_contracts() {
    let l2 = read_fs_meta_spec_file("fs-meta/specs/L2-ARCHITECTURE.md");
    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKER_RUNTIME_SUPPORT.md");
    let transport = read_fs_meta_spec_file("fs-meta/runtime-support/src/transport.rs");
    let typed_client = read_fs_meta_spec_file("fs-meta/runtime-support/src/typed_client.rs");
    let errors = read_fs_meta_spec_file("fs-meta/runtime-support/src/error.rs");

    assert!(l2.contains(
        "`fs-meta/runtime-support/` is the only crate that owns worker child-process bootstrap"
    ));
    assert!(l2.contains("MUST preserve canonical `Timeout` / `TransportClosed` categories plus wall-clock timeout clipping"));
    assert!(l3.contains("ExternalWorkerBootstrapTransport"));
    assert!(l3.contains("ExternalWorkerRetryAndErrorClassification"));
    assert!(l3.contains("direct control-plane startup handshake"));
    assert!(l3.contains("`Ping` / `Init` / `Start` / `Close` / `OnControlFrame`"));
    assert!(transport.contains("spawn_worker_process("));
    assert!(transport.contains("on_control_frame("));
    assert!(transport.contains(".arg(\"--worker-control-socket\")"));
    assert!(transport.contains(".arg(\"--worker-data-socket\")"));
    assert!(transport.contains("control_socket_path"));
    assert!(transport.contains("data_socket_path"));
    assert!(typed_client.contains("effective_rpc_timeout(deadline, rpc_timeout)"));
    assert!(typed_client.contains("control_frames(&envelopes, attempt_timeout)"));
    assert!(errors.contains("CnxError::Timeout => CnxError::Timeout"));
    assert!(errors.contains("CnxError::TransportClosed"));
}

// @verify_spec("CONTRACTS.APP_SCOPE.WORKER_ROLE_MODEL", mode="system")
#[test]
fn test_scan_worker_alias_bootstrap_contract() {
    let l2 = read_fs_meta_spec_file("fs-meta/specs/L2-ARCHITECTURE.md");
    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKER_RUNTIME_SUPPORT.md");
    let scan_lib = read_fs_meta_spec_file("fs-meta/worker-scan/src/lib.rs");
    let scan_main = read_fs_meta_spec_file("fs-meta/worker-scan/src/main.rs");
    let orchestration = read_fs_meta_spec_file("fs-meta/app/src/runtime/orchestration.rs");
    let config = read_fs_meta_spec_file("fs-meta/app/src/lib.rs");

    assert!(l2.contains("`scan-worker` is a distinct operator-visible worker role"));
    assert!(l2.contains("dedicated `run_scan_worker_server(...)` entry while still sharing lower-level source-runtime helpers internally"));
    assert!(l3.contains("ScanWorkerAliasBootstrap"));
    assert!(l3.contains("reuses lower-level source-runtime helpers internally via `run_scan_worker_runtime_loop(...)`"));
    assert!(scan_lib.contains("run_scan_worker_server("));
    assert!(scan_lib.contains("run_scan_worker_runtime_loop(boundary, io_boundary, &runtime)"));
    assert!(scan_main.contains("run_scan_worker_server("));
    assert!(scan_main.contains("&control_socket_path"));
    assert!(scan_main.contains("&data_socket_path"));
    assert!(orchestration.contains("SOURCE_SCAN_RUNTIME_UNIT_ID => Some(SourceRuntimeUnit::Scan)"));
    assert!(config.contains("workers.source.mode and workers.scan.mode must match while source-worker and scan-worker still share one realization"));
}

// @verify_spec("CONTRACTS.API_BOUNDARY.ONLINE_ROOT_RECONFIG_WITHOUT_RESTART", mode="system")
#[test]
fn test_online_root_reconfig_without_restart_contract() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("ONLINE_ROOT_RECONFIG_WITHOUT_RESTART"));
    assert_domain_fs_meta_contract_test(
        "CONTRACTS.API_BOUNDARY.ONLINE_ROOT_RECONFIG_WITHOUT_RESTART",
        "test_online_root_reconfig_without_restart_contract",
    );
}

// @verify_spec("CONTRACTS.API_BOUNDARY.ROOT_PREVIEW_BEFORE_APPLY", mode="system")
#[test]
fn test_root_preview_before_apply_contract() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/API_HTTP.md");
    assert!(l1.contains("ROOT_PREVIEW_BEFORE_APPLY"));
    assert!(l3.contains("POST /monitoring/roots/preview"));
    assert!(l3.contains("unmatched_roots"));
    assert_domain_fs_meta_contract_test(
        "CONTRACTS.API_BOUNDARY.ROOT_PREVIEW_BEFORE_APPLY",
        "test_root_preview_before_apply_contract",
    );
}

#[test]
fn test_runtime_grants_and_monitoring_api_boundary_contract() {
    let l2 = read_fs_meta_spec_file("fs-meta/specs/L2-ARCHITECTURE.md");
    assert!(l2.contains("runtime grants"));
    assert!(l2.contains("monitoring roots"));

    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/API_HTTP.md");
    assert!(l3.contains("POST /session/login"));
    assert!(l3.contains("GET /runtime/grants"));
    assert!(l3.contains("GET  /monitoring/roots"));
    assert!(l3.contains("POST /monitoring/roots/preview"));
    assert!(l3.contains("POST /index/rescan"));

    let api_server = read_fs_meta_spec_file("fs-meta/app/src/api/server.rs");
    assert!(api_server.contains("/api/fs-meta/v1/runtime/grants"));
    assert!(api_server.contains("/api/fs-meta/v1/monitoring/roots"));
    assert!(api_server.contains("/api/fs-meta/v1/monitoring/roots/preview"));
    assert!(api_server.contains("/api/fs-meta/v1/index/rescan"));
    assert!(!api_server.contains("/api/fs-meta/v1/fanout"));
}

// @verify_spec("CONTRACTS.EVOLUTION_AND_OPERATIONS.BINARY_APP_STARTUP_PATH", mode="system")
#[test]
fn test_binary_app_startup_path() {
    if cfg!(not(target_os = "linux")) {
        eprintln!(
            "skip on non-linux: fs-meta startup success path requires linux realtime watcher"
        );
        return;
    }
    assert_l1_contract("CONTRACTS.EVOLUTION_AND_OPERATIONS.BINARY_APP_STARTUP_PATH");
    assert_runtime_orchestrated_multiprocess_config_apply_e2e();
}

// @verify_spec("CONTRACTS.EVOLUTION_AND_OPERATIONS.INDEPENDENT_UPGRADE_CONTINUITY", mode="system")
#[test]
fn test_independent_upgrade_continuity() {
    let cfg_a =
        load_yaml("fs-meta/testdata/specs/fs-meta-contract-tests.cluster.node-a.config.yaml");
    let cfg_b =
        load_yaml("fs-meta/testdata/specs/fs-meta-contract-tests.cluster.node-b.config.yaml");
    let app_decl = load_yaml("fs-meta/testdata/specs/fs-meta.release-v2.yaml");
    let app_count_a = app_count(&cfg_a, "capanix-app-fs-meta");
    let app_count_b = app_count(&cfg_b, "capanix-app-fs-meta");
    let app_count_decl = app_count(&app_decl, "capanix-app-fs-meta");
    assert_eq!(app_count_a, 0);
    assert_eq!(app_count_b, 0);
    assert_eq!(app_count_decl, 1);
}

// @verify_spec("CONTRACTS.EVOLUTION_AND_OPERATIONS.PRODUCT_CONFIGURATION_SPLIT", mode="system")
#[test]
fn test_product_configuration_split_contract() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/API_HTTP.md");
    let deploy = read_fs_meta_spec_file("fs-meta/docs/PRODUCT_DEPLOYMENT.md");
    assert!(l1.contains("PRODUCT_CONFIGURATION_SPLIT"));
    assert!(l3.contains("ProductConfigSurfaceSplit"));
    assert!(
        deploy.contains("业务参数分层")
            && deploy.contains("`fsmeta deploy` 默认以 `roots=[]` 启动服务")
            && deploy.contains("runtime grants -> monitoring roots preview/apply")
    );
}

// @verify_spec("CONTRACTS.EVOLUTION_AND_OPERATIONS.RELEASE_GENERATION_CUTOVER", mode="system")
// @verify_spec("CONTRACTS.EVOLUTION_AND_OPERATIONS.OBSERVATION_ELIGIBILITY_BEFORE_EXPOSURE", mode="system")
#[test]
fn test_release_generation_cutover_contract() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKFLOWS.md");
    let deploy = read_fs_meta_spec_file("fs-meta/docs/PRODUCT_DEPLOYMENT.md");
    let l0 = read_fs_meta_spec_file("fs-meta/specs/L0-VISION.md");
    let glossary = read_fs_meta_spec_file("fs-meta/specs/L0-GLOSSARY.md");
    assert!(l1.contains("RELEASE_GENERATION_CUTOVER"));
    assert!(l1.contains("OBSERVATION_ELIGIBILITY_BEFORE_EXPOSURE"));
    assert!(l0.contains("OBSERVATION_ELIGIBILITY_BEFORE_EXPOSURE"));
    assert!(l0.contains("CROSS_RELATION_DRIFT_VISIBILITY"));
    assert!(
        (l3.contains("new generation receives current authoritative truth inputs")
            || l3.contains("new fs-meta binary release generation"))
            && l3.contains("resource-scoped")
            && l3.contains("rebuilds in-memory observation state through scan/audit/rescan")
    );
    assert!(l3.contains("ObservationEligibilityCutover"));
    assert!(l3.contains("observation_eligible"));
    assert!(l3.contains("status/health surfaces expose the current observation evidence"));
    assert!(l3.contains("facade.pending"));
    assert!(l3.contains("initial_audit_completed"));
    assert!(l1.contains("rebuilding in-memory observation/projection state"));
    assert!(l1.contains("reaching app-owned `observation_eligible`"));
    assert!(l1.contains("optional facade-pending retry diagnostics"));
    assert!(l1.contains("trusted external materialized `/tree` and `/stats` exposure"));
    assert!(l1.contains("`/on-demand-force-find` remains a freshness path"));
    assert!(l1.contains("STATEFUL_APP_OBSERVATION_PLANE_OPT_IN"));
    assert!(l1.contains("UNCERTAIN_STATE_MUST_NOT_PROMOTE"));
    assert!(l1.contains("POST_CUTOVER_STALE_OWNER_FENCING"));
    assert!(l3.contains("trusted external materialized `/tree` and `/stats` exposure"));
    assert!(l3.contains("`/on-demand-force-find` remains a freshness path"));
    assert!(!l3.contains("internal query/find/sink exposure"));
    assert!(deploy.contains("升级版本") && deploy.contains("release generation cutover"));
    assert!(deploy.contains("authoritative truth"));
    assert!(deploy.contains("observation_eligible"));
    assert!(deploy.contains("`/on-demand-force-find` 作为 freshness path"));
    assert!(glossary.contains("Release Generation"));
    assert!(glossary.contains("extends root `L0-GLOSSARY` Convergence Vocabulary"));
    assert!(!glossary.contains("Observed Projection Revision"));
}

// @verify_spec("CONTRACTS.EVOLUTION_AND_OPERATIONS.CROSS_RELATION_DRIFT_VISIBILITY", mode="system")
#[test]
fn test_cross_relation_drift_visibility_contract() {
    let l0 = read_fs_meta_spec_file("fs-meta/specs/L0-VISION.md");
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/OBSERVATION_CUTOVER.md");
    assert!(l0.contains("CROSS_RELATION_DRIFT_VISIBILITY"));
    assert!(l1.contains("CROSS_RELATION_DRIFT_VISIBILITY"));
    assert!(l1.contains("degraded or failure evidence"));
    assert!(l3.contains("explicitly not-ready or degraded"));
}

// @verify_spec("CONTRACTS.EVOLUTION_AND_OPERATIONS.SINGLE_ENTRYPOINT_DESIRED_STATE", mode="system")
#[test]
fn test_single_entrypoint_desired_state_contract_fixture() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("SINGLE_ENTRYPOINT_DESIRED_STATE"));

    let cfg_a =
        load_yaml("fs-meta/testdata/specs/fs-meta-contract-tests.cluster.node-a.config.yaml");
    let cfg_b =
        load_yaml("fs-meta/testdata/specs/fs-meta-contract-tests.cluster.node-b.config.yaml");
    let app_decl = load_yaml("fs-meta/testdata/specs/fs-meta.release-v2.yaml");
    let app_count_a = app_count(&cfg_a, "capanix-app-fs-meta");
    let app_count_b = app_count(&cfg_b, "capanix-app-fs-meta");
    let app_count_decl = app_count(&app_decl, "capanix-app-fs-meta");

    assert_eq!(
        app_count_a, 0,
        "entry node baseline fixture should not carry fs-meta release desired-state"
    );
    assert_eq!(
        app_count_b, 0,
        "peer baseline config should not duplicate fs-meta release desired-state"
    );
    assert_eq!(
        app_count_decl, 1,
        "single-entry release document should carry fs-meta desired-state"
    );
}

// @verify_spec("CONTRACTS.EVOLUTION_AND_OPERATIONS.SINGLE_ENTRYPOINT_DESIRED_STATE", mode="system")
#[test]
fn test_single_entrypoint_desired_state_distributed_apply_e2e() {
    if cfg!(not(target_os = "linux")) {
        eprintln!(
            "skip on non-linux: fs-meta distributed release-apply runtime path requires linux realtime watcher"
        );
        return;
    }
    assert_single_entrypoint_distributed_apply_e2e();
}

// @verify_spec("CONTRACTS.EVOLUTION_AND_OPERATIONS.DOMAIN_TRACEABILITY_CHAIN", mode="system")
#[test]
fn test_domain_traceability_chain() {
    let l0 = read_fs_meta_spec_file("fs-meta/specs/L0-VISION.md");
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKFLOWS.md");
    assert!(l0.contains("fs-meta Domain Vision"));
    assert!(l1.contains("CONTRACTS."));
    assert!(l3.contains("Workflow"));
}

// @verify_spec("CONTRACTS.QUERY_OUTCOME.FORCE_FIND_GROUP_LOCAL_EXCLUSIVITY", mode="system")
#[test]
fn test_force_find_group_local_exclusivity_contract() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("FORCE_FIND_GROUP_LOCAL_EXCLUSIVITY"));

    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKFLOWS.md");
    assert!(l3.contains("round-robin"));
    assert!(l3.contains("in-flight mutex"));

    let source_mod = read_fs_meta_spec_file("fs-meta/app/src/source/mod.rs");
    assert!(
        source_mod.contains("selected_group") || source_mod.contains("failed_roots"),
        "force-find implementation must stay app-owned and route work across source instances"
    );
}

// @verify_spec("CONTRACTS.INDEX_LIFECYCLE.GROUP_SELECTOR_CONFIGURATION", mode="system")
#[test]
fn test_release_yaml_uses_explicit_multi_roots_config() {
    let doc = load_yaml("fs-meta/testdata/specs/fs-meta.release-v2.yaml");
    let app = find_app(&doc, "capanix-app-fs-meta");
    let config = app
        .get(Value::String("config".to_string()))
        .and_then(Value::as_mapping)
        .expect("app config map");
    assert!(
        !config.contains_key(Value::String("root_path".to_string())),
        "single-entry fs-meta release desired-state must not use unsupported root_path"
    );
    let roots = config
        .get(Value::String("roots".to_string()))
        .and_then(Value::as_sequence)
        .expect("roots array");
    assert!(
        roots.len() >= 2,
        "single-entry fs-meta release desired-state should include logical roots"
    );
    assert!(
        roots.iter().all(|v| {
            let m = v.as_mapping().expect("root mapping");
            m.contains_key(Value::String("selector".to_string()))
        }),
        "each root entry must include selector; id remains explicit"
    );
    assert!(
        roots.iter().all(|v| {
            let m = v.as_mapping().expect("root mapping");
            !m.contains_key(Value::String("source_locator".to_string()))
        }),
        "group selector roots must not include legacy source_locator"
    );
}

// @verify_spec("CONTRACTS.EVOLUTION_AND_OPERATIONS.CONTROL_FRAME_SIGNAL_TRANSLATION", mode="system")
#[test]
fn test_control_frame_signal_translation_contract() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("CONTROL_FRAME_SIGNAL_TRANSLATION"));

    let runtime_app = read_fs_meta_spec_file("fs-meta/app/src/runtime_app.rs");
    assert!(runtime_app.contains("split_app_control_signals("));
    assert!(runtime_app.contains("apply_orchestration_signals("));

    let source_mod = read_fs_meta_spec_file("fs-meta/app/src/source/mod.rs");
    assert!(source_mod.contains("source_control_signals_from_envelopes("));
    assert!(source_mod.contains("apply_orchestration_signals(&signals)"));

    let sink_mod = read_fs_meta_spec_file("fs-meta/app/src/sink/mod.rs");
    assert!(sink_mod.contains("sink_control_signals_from_envelopes("));
    assert!(sink_mod.contains("apply_orchestration_signals(&signals)"));
}

// @verify_spec("CONTRACTS.EVOLUTION_AND_OPERATIONS.ORCHESTRATION_TOKEN_PARSING_BOUNDARY", mode="system")
// @verify_spec("CONTRACTS.INDEX_LIFECYCLE.BUSINESS_MODULE_ORCHESTRATION_TOKEN_FREE", mode="system")
#[test]
fn test_orchestration_token_parsing_boundary_contract() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("ORCHESTRATION_TOKEN_PARSING_BOUNDARY"));

    let orchestration = read_fs_meta_spec_file("fs-meta/app/src/runtime/orchestration.rs");
    assert!(orchestration.contains("decode_exec_control_envelope("));
    assert!(orchestration.contains("decode_unit_tick_envelope("));
    assert!(orchestration.contains("decode_runtime_host_object_grants_changed_envelope("));
    assert!(orchestration.contains("source_unit_from_id("));
    assert!(orchestration.contains("sink_unit_from_id("));

    let source_mod = read_fs_meta_spec_file("fs-meta/app/src/source/mod.rs");
    assert!(!source_mod.contains("decode_exec_control_envelope("));
    assert!(!source_mod.contains("decode_unit_tick_envelope("));
    assert!(!source_mod.contains("decode_runtime_host_object_grants_changed_envelope("));

    let sink_mod = read_fs_meta_spec_file("fs-meta/app/src/sink/mod.rs");
    assert!(!sink_mod.contains("decode_exec_control_envelope("));
    assert!(!sink_mod.contains("decode_unit_tick_envelope("));
    assert!(!sink_mod.contains("decode_runtime_host_object_grants_changed_envelope("));
}

// @verify_spec("CONTRACTS.KERNEL_RELATION.HOST_DESCRIPTOR_GROUPING_IS_DOMAIN_POLICY", mode="system")
#[test]
fn test_force_find_all_can_group_by_host_descriptor_policy() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    assert!(l1.contains("HOST_DESCRIPTOR_GROUPING_IS_DOMAIN_POLICY"));
    assert!(l1.contains("host descriptor selectors"));

    let l2 = read_fs_meta_spec_file("fs-meta/specs/L2-ARCHITECTURE.md");
    assert!(l2.contains("runtime host object grants"));
    assert!(l2.contains("descriptor selectors + admin config"));
    assert!(l2.contains("Group Planner"));

    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKFLOWS.md");
    assert!(l3.contains("GroupFormationFromHostObjectDescriptorsWorkflow"));
    assert!(l3.contains("host_ip"));
    assert!(l3.contains("mount_point"));
    assert!(l3.contains("fs_source"));
    assert!(l3.contains("fs_type"));
}

// @verify_spec("CONTRACTS.QUERY_OUTCOME.QUIET_WINDOW_STABILITY_TRACKS_OBSERVED_CHANGE_NOT_REFRESH_NOISE", mode="system")
#[test]
fn test_quiet_window_stability_tracks_observed_change_not_refresh_noise() {
    let l1 = read_fs_meta_spec_file("fs-meta/specs/L1-CONTRACTS.md");
    let l3 = read_fs_meta_spec_file("fs-meta/specs/L3-RUNTIME/WORKFLOWS.md");

    assert!(l1.contains("QUIET_WINDOW_STABILITY_TRACKS_OBSERVED_CHANGE_NOT_REFRESH_NOISE"));
    assert!(l1.contains("observed materialized subtree change"));
    assert!(l1.contains("raw periodic sync traffic or file mtime alone"));
    assert!(l3.contains("## [workflow] QuietWindowStableTreeQuery"));
    assert!(l3.contains("sync-refresh update"));
    assert!(l3.contains("write-significant update"));
    assert!(l3.contains("metadata_mode=stable-only"));
    assert!(l3.contains("`group_page_size/group_after` paginate bucket selection inside one PIT"));
    assert!(l3.contains("`/on-demand-force-find` stays a freshness path"));
}

// @verify_spec("CONTRACTS.QUERY_OUTCOME.GROUP_ORDER_MULTI_GROUP_BUCKET_SELECTION", mode="system")
// @verify_spec("CONTRACTS.QUERY_OUTCOME.DUAL_QUERY_PATH_AVAILABILITY", mode="system")
// @verify_spec("CONTRACTS.QUERY_OUTCOME.NO_CROSS_GROUP_ENTRY_MERGE", mode="system")
// @verify_spec("CONTRACTS.QUERY_OUTCOME.QUERY_HTTP_FACADE_DEFAULTS_AND_SHAPE", mode="system")
// @verify_spec("CONTRACTS.QUERY_OUTCOME.QUERY_BOUND_ROUTE_METRICS_DIAGNOSTICS_BOUNDARY", mode="system")
// @verify_spec("CONTRACTS.QUERY_OUTCOME.QUERY_PARAMETER_AXES_REMAIN_ORTHOGONAL", mode="system")
// @verify_spec("CONTRACTS.API_BOUNDARY.QUERY_PATH_PARAMETERS_OWN_PAYLOAD_SHAPE", mode="system")
// @verify_spec("CONTRACTS.API_BOUNDARY.API_NON_OWNERSHIP_OF_QUERY_FIND_CHANNEL_PATH", mode="system")
#[test]
fn projection_http_runtime_coverage_moved_to_inprocess_projection_api_unit_tests() {
    let projection_api = read_fs_meta_spec_file("fs-meta/app/src/query/api.rs");
    assert!(projection_api.contains("force_find_group_order_file_count_top_bucket_inprocess"));
    assert!(projection_api.contains("force_find_group_order_file_age_top_bucket_inprocess"));
    assert!(projection_api
        .contains("force_find_group_order_file_age_keeps_empty_groups_eligible_inprocess"));
    assert!(
        projection_api.contains("force_find_defaults_to_group_key_multi_group_response_inprocess")
    );
    assert!(projection_api.contains("force_find_defaults_when_query_params_omitted_inprocess"));
    assert!(projection_api.contains("projection_rpc_metrics_endpoint_shape_inprocess"));
    assert!(projection_api
        .contains("force_find_rejects_status_only_and_keeps_pagination_axis_inprocess"));
    assert!(projection_api.contains("namespace_projection_endpoints_removed_inprocess"));
}

#[test]
fn test_single_formal_specs_tree_layout() {
    let root = crate::path_support::fs_meta_root();
    assert!(!root.join("specs/app").exists());
    assert!(!root.join("specs/cli").exists());
    assert!(!root.join("specs/PRODUCT_DEPLOYMENT.md").exists());
    assert!(!root.join("specs/fs-meta-contract-tests.config.md").exists());
    assert!(!root
        .join("specs/fs-meta-contract-tests.cluster.node-a.config.yaml")
        .exists());
    assert!(!root
        .join("specs/fs-meta-contract-tests.cluster.node-b.config.yaml")
        .exists());
    assert!(!root.join("specs/fs-meta.release-v2.yaml").exists());
    assert!(root.join("docs/PRODUCT_DEPLOYMENT.md").exists());
    assert!(root.join("docs/examples/fs-meta.yaml").exists());
    assert!(root
        .join("testdata/specs/fs-meta-contract-tests.config.md")
        .exists());
    assert!(root
        .join("testdata/specs/fs-meta-contract-tests.cluster.node-a.config.yaml")
        .exists());
    assert!(root
        .join("testdata/specs/fs-meta-contract-tests.cluster.node-b.config.yaml")
        .exists());
    assert!(root.join("testdata/specs/fs-meta.release-v2.yaml").exists());
}
