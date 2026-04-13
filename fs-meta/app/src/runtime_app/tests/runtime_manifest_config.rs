#[test]
fn parses_manifest_config_multi_roots() {
    let cfg = std::collections::HashMap::from([
        (
            "roots".to_string(),
            ConfigValue::Array(vec![
                {
                    let mut row = match root_entry_with_id("nfs1-a", "/mnt/nfs1") {
                        ConfigValue::Map(map) => map,
                        _ => unreachable!(),
                    };
                    row.insert("watch".to_string(), ConfigValue::Bool(true));
                    row.insert("scan".to_string(), ConfigValue::Bool(true));
                    ConfigValue::Map(row)
                },
                root_entry_with_id("nfs2-b", "/mnt/nfs2"),
            ]),
        ),
        ("api".to_string(), minimal_api_config()),
    ]);
    let parsed =
        FSMetaProductConfig::from_product_manifest_config(&cfg).expect("parse roots config");
    assert_eq!(parsed.source.roots.len(), 2);
    assert_eq!(parsed.source.roots[0].id, "nfs1-a");
}

#[test]
fn runtime_local_host_ref_from_runtime_metadata() {
    let cfg = std::collections::HashMap::from([(
        "__cnx_runtime".to_string(),
        ConfigValue::Map(std::collections::HashMap::from([(
            "local_host_ref".to_string(),
            ConfigValue::String("host://capanix-node-3".to_string()),
        )])),
    )]);
    let resolved = FSMetaRuntimeApp::runtime_local_host_ref(&cfg)
        .expect("local_host_ref should resolve from runtime metadata");
    assert_eq!(resolved.0, "host://capanix-node-3");
}

#[test]
fn runtime_local_host_ref_rejects_empty_value() {
    let cfg = std::collections::HashMap::from([(
        "__cnx_runtime".to_string(),
        ConfigValue::Map(std::collections::HashMap::from([(
            "local_host_ref".to_string(),
            ConfigValue::String("   ".to_string()),
        )])),
    )]);
    assert!(
        FSMetaRuntimeApp::runtime_local_host_ref(&cfg).is_none(),
        "blank local_host_ref must not be accepted"
    );
}

#[test]
fn required_runtime_local_host_ref_fails_closed_when_missing() {
    let cfg = std::collections::HashMap::from([(
        "__cnx_runtime".to_string(),
        ConfigValue::Map(std::collections::HashMap::new()),
    )]);
    let err = FSMetaRuntimeApp::required_runtime_local_host_ref(&cfg)
        .expect_err("missing local_host_ref must fail closed");
    assert!(
        err.to_string()
            .contains("__cnx_runtime.local_host_ref is required"),
        "error should explain required runtime local_host_ref"
    );
}

#[test]
fn parses_source_tuning_fields_with_bounds() {
    let cfg = std::collections::HashMap::from([
        (
            "roots".to_string(),
            ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
        ),
        ("api".to_string(), minimal_api_config()),
        ("scan_workers".to_string(), ConfigValue::Int(999)),
        ("audit_interval_ms".to_string(), ConfigValue::Int(1)),
        ("throttle_interval_ms".to_string(), ConfigValue::Int(1)),
        ("sink_tombstone_ttl_ms".to_string(), ConfigValue::Int(1)),
        (
            "sink_tombstone_tolerance_us".to_string(),
            ConfigValue::Int(i64::MAX),
        ),
    ]);
    let parsed = FSMetaProductConfig::from_product_manifest_config(&cfg)
        .expect("parse source tuning fields");
    assert_eq!(parsed.source.scan_workers, 16);
    assert_eq!(parsed.source.audit_interval, Duration::from_millis(5_000));
    assert_eq!(parsed.source.throttle_interval, Duration::from_millis(50));
    assert_eq!(
        parsed.source.sink_tombstone_ttl,
        Duration::from_millis(1_000)
    );
    assert_eq!(parsed.source.sink_tombstone_tolerance_us, 10_000_000);
}

#[test]
fn rejects_removed_unit_authority_state_dir() {
    let cfg = std::collections::HashMap::from([
        (
            "roots".to_string(),
            ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
        ),
        ("api".to_string(), minimal_api_config()),
        (
            "unit_authority_state_dir".to_string(),
            ConfigValue::String("relative/statecell".to_string()),
        ),
    ]);
    let err = FSMetaProductConfig::from_product_manifest_config(&cfg)
        .expect_err("removed unit_authority_state_dir must fail");
    assert!(
        err.to_string()
            .contains("unit_authority_state_carrier/unit_authority_state_dir are removed")
    );
}

#[test]
fn rejects_removed_unit_authority_state_carrier() {
    let cfg = std::collections::HashMap::from([
        (
            "roots".to_string(),
            ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
        ),
        ("api".to_string(), minimal_api_config()),
        (
            "unit_authority_state_carrier".to_string(),
            ConfigValue::String("external".to_string()),
        ),
    ]);
    let err = FSMetaProductConfig::from_product_manifest_config(&cfg)
        .expect_err("removed unit_authority_state_carrier must fail");
    assert!(
        err.to_string()
            .contains("unit_authority_state_carrier/unit_authority_state_dir are removed")
    );
}

#[test]
fn rejects_removed_sink_execution_mode_field() {
    let cfg = std::collections::HashMap::from([
        (
            "roots".to_string(),
            ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
        ),
        ("api".to_string(), minimal_api_config()),
        (
            "sink_execution_mode".to_string(),
            ConfigValue::String("external".to_string()),
        ),
    ]);
    let err = FSMetaProductConfig::from_product_manifest_config(&cfg)
        .expect_err("removed sink execution mode must fail");
    assert!(err.to_string().contains(
            "sink_execution_mode has been removed; declare generic workers.<role>.mode/startup.path/socket_dir instead"
        ));
}

#[test]
fn rejects_removed_source_execution_mode_field() {
    let cfg = std::collections::HashMap::from([
        (
            "roots".to_string(),
            ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
        ),
        ("api".to_string(), minimal_api_config()),
        (
            "source_execution_mode".to_string(),
            ConfigValue::String("external".to_string()),
        ),
    ]);
    let err = FSMetaProductConfig::from_product_manifest_config(&cfg)
        .expect_err("removed source execution mode must fail");
    assert!(err.to_string().contains(
            "source_execution_mode has been removed; declare generic workers.<role>.mode/startup.path/socket_dir instead"
        ));
}

#[test]
fn rejects_removed_sink_worker_bin_path_field() {
    let cfg = std::collections::HashMap::from([
        (
            "roots".to_string(),
            ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
        ),
        ("api".to_string(), minimal_api_config()),
        (
            "sink_worker_bin_path".to_string(),
            ConfigValue::String("/usr/local/bin/fs_meta_sink_worker".to_string()),
        ),
    ]);
    let err = FSMetaProductConfig::from_product_manifest_config(&cfg)
        .expect_err("removed sink worker bin path must fail");
    assert!(
        err.to_string()
            .contains("sink_worker_bin_path has been removed")
    );
}

#[test]
fn rejects_removed_source_worker_bin_path_field() {
    let cfg = std::collections::HashMap::from([
        (
            "roots".to_string(),
            ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
        ),
        ("api".to_string(), minimal_api_config()),
        (
            "source_worker_bin_path".to_string(),
            ConfigValue::String("/usr/local/bin/fs_meta_source_worker".to_string()),
        ),
    ]);
    let err = FSMetaProductConfig::from_product_manifest_config(&cfg)
        .expect_err("removed source worker bin path must fail");
    assert!(
        err.to_string()
            .contains("source_worker_bin_path has been removed")
    );
}

#[test]
fn parses_worker_oriented_mode_config_shape() {
    let (source, sink) = runtime_worker_client_bindings(&compiled_runtime_worker_bindings(&[
        ("facade", WorkerMode::Embedded, None),
        (
            "source",
            WorkerMode::External,
            Some("/usr/local/lib/libfs_meta_runtime.so"),
        ),
        ("sink", WorkerMode::Embedded, None),
    ]))
    .expect("parse worker-oriented config");
    assert_eq!(source.mode, WorkerMode::External);
    assert_eq!(sink.mode, WorkerMode::Embedded);
    assert_eq!(
        source.module_path,
        Some(fs_meta_worker_module_path(
            "/usr/local/lib/libfs_meta_runtime.so",
        ))
    );
}

#[test]
fn rejects_missing_compiled_source_worker_binding() {
    let err = runtime_worker_client_bindings(&compiled_runtime_worker_bindings(&[
        ("facade", WorkerMode::Embedded, None),
        (
            "sink",
            WorkerMode::External,
            Some("/usr/local/lib/libfs_meta_runtime.so"),
        ),
    ]))
    .expect_err("missing source binding must fail closed");
    assert!(
        err.to_string()
            .contains("compiled runtime worker bindings must declare role 'source'")
    );
}

#[test]
fn rejects_missing_compiled_sink_worker_binding() {
    let err = runtime_worker_client_bindings(&compiled_runtime_worker_bindings(&[
        ("facade", WorkerMode::Embedded, None),
        (
            "source",
            WorkerMode::External,
            Some("/usr/local/lib/libfs_meta_runtime.so"),
        ),
    ]))
    .expect_err("missing sink binding must fail closed");
    assert!(
        err.to_string()
            .contains("compiled runtime worker bindings must declare role 'sink'")
    );
}

#[test]
fn rejects_external_facade_worker_mode() {
    let err = runtime_worker_client_bindings(&compiled_runtime_worker_bindings(&[(
        "facade",
        WorkerMode::External,
        Some("/tmp/lib.so"),
    )]))
    .expect_err("facade-worker cannot yet move to external mode");
    assert!(
        err.to_string()
            .contains("runtime worker binding for 'facade' must remain embedded")
    );
}

#[test]
fn rejects_missing_compiled_facade_worker_binding() {
    let err = runtime_worker_client_bindings(&compiled_runtime_worker_bindings(&[
        (
            "source",
            WorkerMode::External,
            Some("/usr/local/lib/libfs_meta_runtime.so"),
        ),
        (
            "sink",
            WorkerMode::External,
            Some("/usr/local/lib/libfs_meta_runtime.so"),
        ),
    ]))
    .expect_err("missing facade binding must fail closed");
    assert!(
        err.to_string()
            .contains("compiled runtime worker bindings must declare role 'facade'")
    );
}

#[test]
fn rejects_root_source_locator_field() {
    let cfg = std::collections::HashMap::from([(
        "roots".to_string(),
        ConfigValue::Array(vec![{
            let mut row = match root_entry_with_id("nfs1", "/mnt/nfs1") {
                ConfigValue::Map(map) => map,
                _ => unreachable!(),
            };
            row.insert(
                "source_locator".to_string(),
                ConfigValue::String("10.0.0.11".to_string()),
            );
            ConfigValue::Map(row)
        }]),
    )]);
    let err = FSMetaProductConfig::from_product_manifest_config(&cfg)
        .expect_err("must reject source_locator");
    assert!(err.to_string().contains("source_locator is forbidden"));
}

#[test]
fn derives_root_id_from_selector_mount_point_when_missing() {
    let cfg = std::collections::HashMap::from([
        (
            "roots".to_string(),
            ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
        ),
        ("api".to_string(), minimal_api_config()),
    ]);
    let parsed =
        FSMetaProductConfig::from_product_manifest_config(&cfg).expect("parse roots config");
    assert_eq!(parsed.source.roots[0].id, "mnt-nfs1");
}

#[test]
fn rejects_root_path_field() {
    let cfg = std::collections::HashMap::from([(
        "root_path".to_string(),
        ConfigValue::String("/mnt/nfs1".to_string()),
    )]);
    let err =
        FSMetaProductConfig::from_product_manifest_config(&cfg).expect_err("must reject root_path");
    assert!(err.to_string().contains("root_path is forbidden"));
}

#[test]
fn allows_missing_roots_field_as_empty_deployed_state() {
    let cfg = std::collections::HashMap::from([("api".to_string(), minimal_api_config())]);
    let parsed = FSMetaProductConfig::from_product_manifest_config(&cfg)
        .expect("missing roots should parse as empty deployed state");
    assert!(parsed.source.roots.is_empty());
}

#[test]
fn parses_api_config_auth_fields() {
    let cfg = std::collections::HashMap::from([
        (
            "roots".to_string(),
            ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
        ),
        (
            "api".to_string(),
            ConfigValue::Map(std::collections::HashMap::from([
                ("enabled".to_string(), ConfigValue::Bool(true)),
                (
                    "facade_resource_id".to_string(),
                    ConfigValue::String("fs-meta-tcp-listener".to_string()),
                ),
                (
                    "auth".to_string(),
                    ConfigValue::Map(std::collections::HashMap::from([
                        (
                            "passwd_path".to_string(),
                            ConfigValue::String("/tmp/fs-meta.passwd".to_string()),
                        ),
                        (
                            "shadow_path".to_string(),
                            ConfigValue::String("/tmp/fs-meta.shadow".to_string()),
                        ),
                        ("session_ttl_secs".to_string(), ConfigValue::Int(900)),
                        (
                            "query_keys_path".to_string(),
                            ConfigValue::String("/tmp/fs-meta.query-keys.json".to_string()),
                        ),
                        (
                            "management_group".to_string(),
                            ConfigValue::String("fsmeta_management".to_string()),
                        ),
                    ])),
                ),
            ])),
        ),
    ]);
    let cfg = {
        let mut cfg = cfg;
        cfg.insert(
            "__cnx_runtime".to_string(),
            ConfigValue::Map(std::collections::HashMap::from([
                (
                    "local_host_ref".to_string(),
                    ConfigValue::String("host://capanix-node-3".to_string()),
                ),
                (
                    "announced_resources".to_string(),
                    ConfigValue::Array(vec![ConfigValue::Map(std::collections::HashMap::from([
                        (
                            "resource_id".to_string(),
                            ConfigValue::String("fs-meta-tcp-listener".to_string()),
                        ),
                        (
                            "node_id".to_string(),
                            ConfigValue::String("host://capanix-node-3".to_string()),
                        ),
                        (
                            "resource_kind".to_string(),
                            ConfigValue::String("tcp_listener".to_string()),
                        ),
                        (
                            "bind_addr".to_string(),
                            ConfigValue::String("127.0.0.1:18080".to_string()),
                        ),
                    ]))]),
                ),
            ])),
        );
        cfg
    };
    let parsed = FSMetaConfig::from_runtime_manifest_config(&cfg).expect("parse api config");
    assert!(parsed.api.enabled);
    assert_eq!(parsed.api.facade_resource_id, "fs-meta-tcp-listener");
    assert_eq!(parsed.api.local_listener_resources.len(), 1);
    assert_eq!(
        parsed.api.local_listener_resources[0].bind_addr,
        "127.0.0.1:18080"
    );
    assert_eq!(parsed.api.auth.session_ttl_secs, 900);
    assert_eq!(
        parsed.api.auth.passwd_path,
        std::path::PathBuf::from("/tmp/fs-meta.passwd")
    );
    assert_eq!(
        parsed.api.auth.query_keys_path,
        std::path::PathBuf::from("/tmp/fs-meta.query-keys.json")
    );
    assert_eq!(parsed.api.auth.management_group, "fsmeta_management");
}

#[test]
fn rejects_api_disabled_in_manifest_config() {
    let cfg = std::collections::HashMap::from([
        (
            "roots".to_string(),
            ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
        ),
        (
            "api".to_string(),
            ConfigValue::Map(std::collections::HashMap::from([(
                "enabled".to_string(),
                ConfigValue::Bool(false),
            )])),
        ),
    ]);
    let mut cfg = cfg;
    if let Some(ConfigValue::Map(api)) = cfg.get_mut("api") {
        api.insert(
            "auth".to_string(),
            ConfigValue::Map(std::collections::HashMap::new()),
        );
    }
    let err =
        FSMetaProductConfig::from_product_manifest_config(&cfg).expect_err("api must be mandatory");
    assert!(
        err.to_string()
            .contains("api.enabled must be true; fs-meta management API boundary is mandatory")
    );
}
