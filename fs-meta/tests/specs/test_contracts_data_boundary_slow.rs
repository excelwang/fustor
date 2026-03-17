// ASSERTION ROUTE (Black-box tests — public traits and APIs only).
// Do not introduce white-box testing logic or internal workarounds.
//
// Verifies: L1 CONTRACTS.DATA_BOUNDARY

use std::fs;
use std::time::{Duration, Instant};

use crate::common::assert_cluster_bootstrap_governance_real_cluster;
use bytes::Bytes;
use capanix_app_fs_meta::query::models::QueryNode;
use capanix_app_fs_meta::{FSMetaApp, FSMetaConfig};
use capanix_app_sdk::runtime::{ConfigValue, EventMetadata, NodeId, RecvOpts};
use capanix_app_sdk::{Event, RuntimeBoundaryApp};
use capanix_host_fs_types::{FileMetaRecord, UnixStat};
use capanix_route_proto::{
    encode_exec_control_envelope, now_ms, BoundScope, ExecActivate, ExecControl,
};
use tempfile::TempDir;

const DATA_BOUNDARY_SCOPE_ID: &str = "data-boundary-root";

async fn offer_primary(app: &impl RuntimeBoundaryApp, route_key: impl Into<String>) {
    let route_key = route_key.into();
    let unit_ids = vec![
        "runtime.exec.source".to_string(),
        "runtime.exec.scan".to_string(),
        "runtime.exec.sink".to_string(),
    ];
    let bound_scopes = vec![BoundScope {
        scope_id: DATA_BOUNDARY_SCOPE_ID.to_string(),
        resource_ids: vec!["data-boundary-root".to_string()],
    }];
    for unit_id in &unit_ids {
        let ctrl = ExecControl::Activate(ExecActivate {
            route_key: route_key.clone(),
            worker_id: unit_id.clone(),
            lease: None,
            generation: 1,
            expires_at_ms: now_ms() + 60_000,
            bound_scopes: bound_scopes.clone(),
        });
        let env = encode_exec_control_envelope(&ctrl).expect("encode exec activation");
        app.on_control_frame(&[env])
            .await
            .expect("apply exec activation via control");
    }
}

fn write_auth_files(dir: &TempDir) -> (String, String) {
    let passwd_path = dir.path().join("fs-meta.passwd");
    let shadow_path = dir.path().join("fs-meta.shadow");
    fs::write(
        &passwd_path,
        "admin:1000:1000:fsmeta_management:/home/admin:/bin/bash:0\n",
    )
    .expect("write passwd");
    fs::write(&shadow_path, "admin:plain$admin:0\n").expect("write shadow");
    (
        passwd_path.display().to_string(),
        shadow_path.display().to_string(),
    )
}

async fn make_app(dir: &TempDir, node_id: &str) -> FSMetaApp {
    let (passwd_path, shadow_path) = write_auth_files(dir);
    let cfg_map = std::collections::HashMap::from([
        (
            "roots".to_string(),
            ConfigValue::Array(vec![ConfigValue::Map(
                std::collections::HashMap::from([
                    (
                        "id".to_string(),
                        ConfigValue::String(DATA_BOUNDARY_SCOPE_ID.to_string()),
                    ),
                    (
                        "selector".to_string(),
                        ConfigValue::Map(std::collections::HashMap::from([(
                            "mount_point".to_string(),
                            ConfigValue::String(dir.path().display().to_string()),
                        )])),
                    ),
                    (
                        "subpath_scope".to_string(),
                        ConfigValue::String("/".to_string()),
                    ),
                    (
                        "watch".to_string(),
                        ConfigValue::Bool(true),
                    ),
                    ("scan".to_string(), ConfigValue::Bool(true)),
                ]),
            )]),
        ),
        (
            "api".to_string(),
            ConfigValue::Map(std::collections::HashMap::from([
                (
                    "enabled".to_string(),
                    ConfigValue::Bool(true),
                ),
                (
                    "facade_resource_id".to_string(),
                    ConfigValue::String(
                        "root-data-boundary-facade-resource".to_string(),
                    ),
                ),
                (
                    "auth".to_string(),
                    ConfigValue::Map(std::collections::HashMap::from([
                        (
                            "passwd_path".to_string(),
                            ConfigValue::String(passwd_path),
                        ),
                        (
                            "shadow_path".to_string(),
                            ConfigValue::String(shadow_path),
                        ),
                    ])),
                ),
            ])),
        ),
        (
            "__cnx_runtime".to_string(),
            ConfigValue::Map(std::collections::HashMap::from([
                (
                    "local_host_ref".to_string(),
                    ConfigValue::String(node_id.to_string()),
                ),
                (
                    "announced_resources".to_string(),
                    ConfigValue::Array(vec![ConfigValue::Map(
                        std::collections::HashMap::from([
                            (
                                "resource_id".to_string(),
                                ConfigValue::String(
                                    "root-data-boundary-facade-resource".to_string(),
                                ),
                            ),
                            (
                                "resource_kind".to_string(),
                                ConfigValue::String("tcp_listener".to_string()),
                            ),
                            (
                                "source".to_string(),
                                ConfigValue::String(
                                    "http://root-app-node/facade-resource/root-data-boundary-facade-resource"
                                        .to_string(),
                                ),
                            ),
                            (
                                "bind_addr".to_string(),
                                ConfigValue::String("127.0.0.1:0".to_string()),
                            ),
                        ]),
                    )]),
                ),
                (
                    "host_object_grants".to_string(),
                    ConfigValue::Array(vec![ConfigValue::Map(
                        std::collections::HashMap::from([
                            (
                                "object_ref".to_string(),
                                ConfigValue::String(node_id.to_string()),
                            ),
                            (
                                "host_ref".to_string(),
                                ConfigValue::String(node_id.to_string()),
                            ),
                            (
                                "interfaces".to_string(),
                                ConfigValue::Array(vec![
                                    ConfigValue::String("posix-fs".to_string()),
                                    ConfigValue::String("inotify".to_string()),
                                ]),
                            ),
                            (
                                "host_descriptors".to_string(),
                                ConfigValue::Map(std::collections::HashMap::from(
                                    [
                                        (
                                            "host_ip".to_string(),
                                            ConfigValue::String(
                                                "127.0.0.1".to_string(),
                                            ),
                                        ),
                                        (
                                            "host_name".to_string(),
                                            ConfigValue::String(
                                                node_id.to_string(),
                                            ),
                                        ),
                                    ],
                                )),
                            ),
                            (
                                "object_descriptors".to_string(),
                                ConfigValue::Map(std::collections::HashMap::from(
                                    [
                                        (
                                            "mount_point".to_string(),
                                            ConfigValue::String(
                                                dir.path().display().to_string(),
                                            ),
                                        ),
                                        (
                                            "fs_source".to_string(),
                                            ConfigValue::String(format!(
                                                "fixture:{node_id}"
                                            )),
                                        ),
                                        (
                                            "fs_type".to_string(),
                                            ConfigValue::String(
                                                "fixture".to_string(),
                                            ),
                                        ),
                                    ],
                                )),
                            ),
                            (
                                "grant_state".to_string(),
                                ConfigValue::String("active".to_string()),
                            ),
                        ]),
                    )]),
                ),
                (
                    "app_scopes".to_string(),
                    ConfigValue::Array(vec![ConfigValue::Map(
                        std::collections::HashMap::from([
                            (
                                "scope_id".to_string(),
                                ConfigValue::String(
                                    DATA_BOUNDARY_SCOPE_ID.to_string(),
                                ),
                            ),
                            (
                                "unit_scopes".to_string(),
                                ConfigValue::Array(vec![
                                    ConfigValue::Map(
                                        std::collections::HashMap::from([
                                            (
                                                "unit_id".to_string(),
                                                ConfigValue::String(
                                                    "runtime.exec.source".to_string(),
                                                ),
                                            ),
                                            (
                                                "resource_ids".to_string(),
                                                ConfigValue::Int(1),
                                            ),
                                        ]),
                                    ),
                                    ConfigValue::Map(
                                        std::collections::HashMap::from([
                                            (
                                                "unit_id".to_string(),
                                                ConfigValue::String(
                                                    "runtime.exec.scan".to_string(),
                                                ),
                                            ),
                                            (
                                                "resource_ids".to_string(),
                                                ConfigValue::Int(1),
                                            ),
                                        ]),
                                    ),
                                    ConfigValue::Map(
                                        std::collections::HashMap::from([
                                            (
                                                "unit_id".to_string(),
                                                ConfigValue::String(
                                                    "runtime.exec.sink".to_string(),
                                                ),
                                            ),
                                            (
                                                "resource_ids".to_string(),
                                                ConfigValue::Int(1),
                                            ),
                                        ]),
                                    ),
                                ]),
                            ),
                        ]),
                    )]),
                ),
            ])),
        ),
        (
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
        ),
    ]);
    let mut cfg = FSMetaConfig::from_manifest_config(&cfg_map).expect("parse fs-meta config");
    cfg.source.batch_size = 64;
    cfg.source.scan_workers = 2;
    cfg.source.lru_capacity = 1024;
    let app = FSMetaApp::new(cfg, NodeId(node_id.to_string())).expect("init fs-meta app");
    offer_primary(&app, format!("root-data-boundary-{node_id}")).await;
    app.start().await.expect("start fs-meta app");
    let deadline = Instant::now() + Duration::from_secs(12);
    loop {
        match app.recv(RecvOpts::default()).await {
            Ok(events) if !events.is_empty() => break,
            _ => {}
        }
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    app
}

fn make_source_event(origin: &str, path: &[u8], size: u64, timestamp_us: u64) -> Event {
    let file_name = std::path::Path::new(std::str::from_utf8(path).expect("utf8 path"))
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("")
        .to_string();
    let payload = rmp_serde::to_vec(&FileMetaRecord::scan_update(
        path.to_vec(),
        file_name.into_bytes(),
        UnixStat {
            is_dir: false,
            size,
            mtime_us: timestamp_us,
            ctime_us: timestamp_us,
            dev: None,
            ino: None,
        },
        b"/".to_vec(),
        timestamp_us,
        false,
    ))
    .expect("encode file meta record");
    Event::new(
        EventMetadata {
            origin_id: NodeId(origin.to_string()),
            timestamp_us,
            logical_ts: None,
            correlation_id: None,
            ingress_auth: None,
            trace: None,
        },
        Bytes::from(payload),
    )
}

// @verify_spec("CONTRACTS.DATA_BOUNDARY.TYPED_EVENT_CONTINUITY", mode="system")
#[tokio::test]
async fn test_source_snapshot_commits_into_sink_query_shape() {
    if cfg!(not(target_os = "linux")) {
        eprintln!("skip on non-linux: fs-meta source realtime contract is linux-only");
        return;
    }
    assert_cluster_bootstrap_governance_real_cluster();

    let dir = TempDir::new().unwrap();
    fs::write(dir.path().join("alpha.txt"), b"alpha").unwrap();

    let node_id = "root-app-node-a";
    let app = make_app(&dir, node_id).await;
    app.send(&[make_source_event(
        node_id,
        b"/alpha.txt",
        5,
        1_700_000_000_000_111,
    )])
    .await
    .expect("inject source-origin snapshot event");
    let deadline = Instant::now() + Duration::from_secs(25);
    let node = loop {
        if let Some(node) = app.query_node(b"/alpha.txt").unwrap() {
            break node;
        }
        if Instant::now() >= deadline {
            panic!("expected query node for /alpha.txt");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    assert_eq!(node.path, b"/alpha.txt");
    assert_eq!(node.size, 5);
    assert!(node.modified_time_us > 0);
    assert!(!node.is_dir);

    app.close().await.unwrap();
}

// @verify_spec("CONTRACTS.DATA_BOUNDARY.METADATA_CONTINUITY", mode="system")
#[tokio::test]
async fn test_sink_shadow_clock_tracks_source_event_timestamps() {
    if cfg!(not(target_os = "linux")) {
        eprintln!("skip on non-linux: fs-meta source realtime contract is linux-only");
        return;
    }
    assert_cluster_bootstrap_governance_real_cluster();

    let dir = TempDir::new().unwrap();
    fs::write(dir.path().join("one.txt"), b"1").unwrap();
    fs::write(dir.path().join("two.txt"), b"22").unwrap();

    let node_id = "root-app-node-b";
    let app = make_app(&dir, node_id).await;
    app.send(&[
        make_source_event(node_id, b"/one.txt", 1, 1_700_000_000_000_101),
        make_source_event(node_id, b"/two.txt", 2, 1_700_000_000_000_222),
    ])
    .await
    .expect("inject source-origin events");
    let node_deadline = Instant::now() + Duration::from_secs(25);
    loop {
        let one = app.query_node(b"/one.txt").unwrap();
        let two = app.query_node(b"/two.txt").unwrap();
        if one.is_some() && two.is_some() {
            break;
        }
        if Instant::now() >= node_deadline {
            panic!("expected materialized nodes for /one.txt and /two.txt");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let deadline = Instant::now() + Duration::from_secs(25);
    let (stable_origin_id, max_ts) = loop {
        let queried = app.recv(RecvOpts::default()).await.unwrap();
        assert!(
            !queried.is_empty(),
            "app query must return materialized data"
        );
        let stable_origin_id = queried[0].metadata().origin_id.clone();
        let max_ts = queried
            .iter()
            .map(|e| {
                assert_eq!(
                    e.metadata().origin_id,
                    stable_origin_id,
                    "query metadata origin_id must stay stable per logical query snapshot"
                );
                e.metadata().timestamp_us
            })
            .max()
            .unwrap_or(0);
        if max_ts > 0 {
            break (stable_origin_id, max_ts);
        }
        if Instant::now() >= deadline {
            panic!("query metadata timestamp_us must be populated");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    let queried_again = app.recv(RecvOpts::default()).await.unwrap();
    for event in queried_again {
        assert_eq!(
            event.metadata().origin_id,
            stable_origin_id,
            "query metadata origin_id must remain stable without new ingest"
        );
        assert_eq!(
            event.metadata().timestamp_us,
            max_ts,
            "query metadata timestamp must remain stable without new ingest"
        );
    }

    app.close().await.unwrap();
}

// @verify_spec("CONTRACTS.DATA_BOUNDARY.REFERENCE_DOMAIN_SLICE_CONNECTIVITY", mode="system")
#[tokio::test]
async fn test_reference_domain_slice_connectivity_fs_meta() {
    if cfg!(not(target_os = "linux")) {
        eprintln!("skip on non-linux: fs-meta source realtime contract is linux-only");
        return;
    }
    assert_cluster_bootstrap_governance_real_cluster();

    let dir = TempDir::new().unwrap();
    fs::write(dir.path().join("slice-a.txt"), b"a").unwrap();
    fs::write(dir.path().join("slice-b.txt"), b"bb").unwrap();

    let node_id = "root-app-node-slice";
    let app = make_app(&dir, node_id).await;
    app.send(&[
        make_source_event(node_id, b"/slice-a.txt", 1, 1_700_000_000_000_301),
        make_source_event(node_id, b"/slice-b.txt", 2, 1_700_000_000_000_302),
    ])
    .await
    .expect("inject source-origin slice events");
    let deadline = Instant::now() + Duration::from_secs(25);
    let (slice_a, slice_b): (QueryNode, QueryNode) = loop {
        let slice_a = app.query_node(b"/slice-a.txt").unwrap();
        let slice_b = app.query_node(b"/slice-b.txt").unwrap();
        if let (Some(a), Some(b)) = (slice_a, slice_b) {
            break (a, b);
        }
        if Instant::now() >= deadline {
            panic!("expected materialized nodes for /slice-a.txt and /slice-b.txt");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(slice_a.path, b"/slice-a.txt");
    assert_eq!(slice_b.path, b"/slice-b.txt");

    app.close().await.unwrap();
}
