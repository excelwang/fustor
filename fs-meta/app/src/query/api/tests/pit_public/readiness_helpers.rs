#[test]
fn materialized_owner_node_for_group_prefers_scheduled_owner_when_primary_object_ref_is_unscheduled(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a");
    let root_b = tmp.path().join("node-b");
    fs::create_dir_all(&root_a).expect("create node-a dir");
    fs::create_dir_all(&root_b).expect("create node-b dir");

    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs4".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a.clone(),
            fs_source: "server:/nfs4".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs4".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b.clone(),
            fs_source: "server:/nfs4".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(vec![RootSpec::new("nfs4", &root_a)], &grants);
    let sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs4".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs4".to_string(),
            primary_object_ref: "node-a::nfs4".to_string(),
            total_nodes: 1,
            live_nodes: 1,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 1,
            shadow_lag_us: 0,
            overflow_pending_audit: false,
            initial_audit_completed: true,
            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };

    let owner = crate::runtime_app::shared_tokio_runtime()
        .block_on(materialized_owner_node_for_group(
            source.as_ref(),
            Some(&sink_status),
            "nfs4",
        ))
        .expect("resolve nfs4 owner")
        .expect("nfs4 owner");

    assert_eq!(
        owner.0, "node-b",
        "scheduled sink owner should win when primary_object_ref names a node that is no longer scheduled for the group"
    );
}

#[test]
fn materialized_owner_node_for_group_tracks_group_primary_by_group() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a");
    let root_b = tmp.path().join("node-b");
    fs::create_dir_all(&root_a).expect("create node-a dir");
    fs::create_dir_all(&root_b).expect("create node-b dir");

    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a.clone(),
            fs_source: "server:/nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs2".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b.clone(),
            fs_source: "server:/nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", &root_a),
            RootSpec::new("nfs2", &root_b),
        ],
        &grants,
    );

    let nfs1 = crate::runtime_app::shared_tokio_runtime()
        .block_on(materialized_owner_node_for_group(
            source.as_ref(),
            None,
            "nfs1",
        ))
        .expect("resolve nfs1 owner")
        .expect("nfs1 owner");
    let nfs2 = crate::runtime_app::shared_tokio_runtime()
        .block_on(materialized_owner_node_for_group(
            source.as_ref(),
            None,
            "nfs2",
        ))
        .expect("resolve nfs2 owner")
        .expect("nfs2 owner");

    assert_eq!(nfs1.0, "node-a");
    assert_eq!(nfs2.0, "node-b");
}

#[test]
fn materialized_owner_node_for_group_does_not_fallback_to_source_primary_when_explicit_sink_status_omits_group()
{
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a");
    let root_b = tmp.path().join("node-b");
    fs::create_dir_all(&root_a).expect("create node-a dir");
    fs::create_dir_all(&root_b).expect("create node-b dir");

    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a.clone(),
            fs_source: "server:/nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs2".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b.clone(),
            fs_source: "server:/nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", &root_a),
            RootSpec::new("nfs2", &root_b),
        ],
        &grants,
    );
    let sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs2".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs2".to_string(),
            primary_object_ref: "node-b::nfs2".to_string(),
            total_nodes: 1,
            live_nodes: 1,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 1,
            shadow_lag_us: 0,
            overflow_pending_audit: false,
            initial_audit_completed: true,
            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };

    let owner = crate::runtime_app::shared_tokio_runtime()
        .block_on(materialized_owner_node_for_group(
            source.as_ref(),
            Some(&sink_status),
            "nfs1",
        ))
        .expect("resolve nfs1 owner");

    assert!(
        owner.is_none(),
        "explicit selected-group sink status that omits a group must not fall back to source_primary_by_group snapshot for trusted materialized owner routing"
    );
}

#[test]
fn resolve_force_find_groups_uses_local_source_snapshot_and_filters_scan_disabled_roots() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("nfs1");
    let root_b = tmp.path().join("nfs2");
    let root_c = tmp.path().join("nfs3");
    fs::create_dir_all(&root_a).expect("create nfs1 dir");
    fs::create_dir_all(&root_b).expect("create nfs2 dir");
    fs::create_dir_all(&root_c).expect("create nfs3 dir");

    let grants = vec![
        granted_mount_root("node-a::nfs1", &root_a),
        granted_mount_root("node-b::nfs2", &root_b),
        granted_mount_root("node-c::nfs3", &root_c),
    ];
    let mut nfs3 = RootSpec::new("nfs3", &root_c);
    nfs3.scan = false;
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", &root_a),
            RootSpec::new("nfs2", &root_b),
            nfs3,
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let state = test_api_state_for_source(source, sink);
    let params = NormalizedApiParams {
        path: b"/".to_vec(),
        group: None,
        recursive: true,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::default(),
        group_page_size: Some(GROUP_PAGE_SIZE_DEFAULT),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: None,
    };

    let groups = crate::runtime_app::shared_tokio_runtime()
        .block_on(resolve_force_find_groups(&state, &params))
        .expect("resolve groups");

    assert_eq!(groups, vec!["nfs1".to_string(), "nfs2".to_string()]);
}

#[test]
fn resolve_force_find_groups_keeps_scan_enabled_roots_when_primary_snapshot_is_partial() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("nfs1");
    let root_b = tmp.path().join("nfs2");
    let root_c = tmp.path().join("nfs3");
    fs::create_dir_all(&root_a).expect("create nfs1 dir");
    fs::create_dir_all(&root_b).expect("create nfs2 dir");
    fs::create_dir_all(&root_c).expect("create nfs3 dir");

    // Simulate node-local grants that only include one group while all scan-enabled roots are present.
    let grants = vec![granted_mount_root("node-a::nfs1", &root_a)];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", &root_a),
            RootSpec::new("nfs2", &root_b),
            RootSpec::new("nfs3", &root_c),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let state = test_api_state_for_source(source, sink);
    let params = NormalizedApiParams {
        path: b"/".to_vec(),
        group: None,
        recursive: true,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::default(),
        group_page_size: Some(GROUP_PAGE_SIZE_DEFAULT),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: None,
    };

    let groups = crate::runtime_app::shared_tokio_runtime()
        .block_on(resolve_force_find_groups(&state, &params))
        .expect("resolve groups");

    assert_eq!(
        groups,
        vec!["nfs1".to_string(), "nfs2".to_string(), "nfs3".to_string()],
        "fresh all-groups force-find scope must retain all scan-enabled groups even if source primary snapshot is partial"
    );
}

#[test]
fn query_responses_from_source_events_keeps_prefixed_find_rows() {
    let query_path = b"/qf-e2e-job";
    let events = vec![
        mk_source_record_event(
            "node-a::nfs1",
            b"/tmp/capanix/data/nfs1/qf-e2e-job/file-a.txt",
            b"file-a.txt",
            10,
        ),
        mk_source_record_event(
            "node-a::nfs1",
            b"/tmp/capanix/data/nfs1/qf-e2e-job/file-b.txt",
            b"file-b.txt",
            11,
        ),
    ];

    let grouped = query_responses_by_origin_from_source_events(&events, query_path)
        .expect("build grouped query responses");
    let response = grouped.get("node-a::nfs1").expect("origin response exists");
    let mut paths = response
        .entries
        .iter()
        .map(|node| String::from_utf8_lossy(&node.path).to_string())
        .collect::<Vec<_>>();
    paths.sort();
    assert_eq!(
        paths,
        vec![
            "/qf-e2e-job/file-a.txt".to_string(),
            "/qf-e2e-job/file-b.txt".to_string()
        ]
    );
}

#[test]
fn max_total_files_from_stats_events_uses_highest_total_files() {
    let mk_stats_event = |origin: &str, total_files: u64| -> Event {
        let payload = rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(SubtreeStats {
            total_files,
            ..SubtreeStats::default()
        }))
        .expect("encode stats");
        mk_event(origin, payload)
    };

    let events = vec![mk_stats_event("n1", 3), mk_stats_event("n2", 8)];
    assert_eq!(max_total_files_from_stats_events(&events), Some(8));
}

#[test]
fn latest_file_mtime_from_stats_events_uses_newest_file_mtime() {
    let mk_stats_event = |origin: &str, mtime: Option<u64>| -> Event {
        let payload = rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(SubtreeStats {
            latest_file_mtime_us: mtime,
            ..SubtreeStats::default()
        }))
        .expect("encode stats");
        mk_event(origin, payload)
    };

    let events = vec![
        mk_stats_event("n1", Some(7)),
        mk_stats_event("n2", Some(11)),
        mk_stats_event("n3", None),
        mk_event("n3", b"bad-msgpack".to_vec()),
    ];
    assert_eq!(latest_file_mtime_from_stats_events(&events), Some(11));
}

#[test]
fn error_response_maps_protocol_violation_to_bad_gateway() {
    let response = error_response(CnxError::ProtocolViolation("x".into()));
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
}

#[test]
fn error_response_maps_timeout_to_gateway_timeout() {
    let response = error_response(CnxError::Timeout);
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
}

#[test]
fn error_response_maps_internal_to_internal_server_error() {
    let response = error_response(CnxError::Internal("x".into()));
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[test]
fn error_response_maps_transport_closed_to_service_unavailable() {
    let response = error_response(CnxError::TransportClosed("x".into()));
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[test]
fn error_response_maps_peer_error_to_bad_gateway() {
    let response = error_response(CnxError::PeerError("x".into()));
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
}

#[test]
fn error_response_maps_invalid_input_to_bad_request() {
    let response = error_response(CnxError::InvalidInput("x".into()));
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn error_response_maps_force_find_inflight_conflict_to_too_many_requests() {
    let response = error_response(CnxError::NotReady(format!(
        "{FORCE_FIND_INFLIGHT_CONFLICT_PREFIX} force-find already running for group: nfs1"
    )));
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[test]
fn build_materialized_stats_request_keeps_shape() {
    let params = build_materialized_stats_request(b"/a", true, Some("sink-a".into()));
    assert_eq!(params.op, QueryOp::Stats);
    assert_eq!(params.scope.path, b"/a");
    assert!(params.scope.recursive);
    assert_eq!(params.scope.selected_group.as_deref(), Some("sink-a"));
}

#[test]
fn build_materialized_tree_request_sets_selected_group() {
    let params = build_materialized_tree_request(
        b"/a",
        false,
        Some(2),
        ReadClass::TrustedMaterialized,
        Some("sink-a".to_string()),
    );
    assert_eq!(params.op, QueryOp::Tree);
    assert_eq!(params.scope.path, b"/a");
    assert!(!params.scope.recursive);
    assert_eq!(params.scope.max_depth, Some(2));
    let tree_options = params.tree_options.expect("tree options");
    assert_eq!(tree_options.read_class, ReadClass::TrustedMaterialized);
    assert_eq!(params.scope.selected_group.as_deref(), Some("sink-a"));
}

#[test]
fn normalize_api_params_uses_defaults() {
    let params = ApiParams {
        path: None,
        path_b64: None,
        group: None,
        recursive: None,
        max_depth: None,
        pit_id: None,
        group_order: None,
        group_page_size: None,
        group_after: None,
        entry_page_size: None,
        entry_after: None,
        read_class: None,
    };
    let normalized = normalize_api_params(params).expect("normalize defaults");
    assert_eq!(normalized.path, b"/".to_vec());
    assert_eq!(normalized.group, None);
    assert!(normalized.recursive);
    assert_eq!(normalized.max_depth, None);
    assert_eq!(normalized.pit_id, None);
    assert_eq!(normalized.group_order, GroupOrder::GroupKey);
    assert_eq!(normalized.group_page_size, None);
    assert_eq!(normalized.group_after, None);
    assert_eq!(normalized.entry_page_size, None);
    assert_eq!(normalized.entry_after, None);
    assert_eq!(normalized.read_class, None);
}

#[test]
fn normalize_api_params_keeps_group_pagination_shape() {
    let params = ApiParams {
        path: Some("/mnt".into()),
        path_b64: None,
        group: None,
        recursive: Some(false),
        max_depth: Some(3),
        pit_id: Some("pit-1".into()),
        group_order: Some(GroupOrder::FileAge),
        group_page_size: Some(25),
        group_after: Some("group-cursor-1".into()),
        entry_page_size: Some(100),
        entry_after: Some("entry-cursor-bundle-1".into()),
        read_class: Some(ReadClass::Materialized),
    };
    let normalized = normalize_api_params(params).expect("normalize explicit params");
    assert_eq!(normalized.path, b"/mnt".to_vec());
    assert_eq!(normalized.group, None);
    assert!(!normalized.recursive);
    assert_eq!(normalized.max_depth, Some(3));
    assert_eq!(normalized.pit_id.as_deref(), Some("pit-1"));
    assert_eq!(normalized.group_order, GroupOrder::FileAge);
    assert_eq!(normalized.group_page_size, Some(25));
    assert_eq!(normalized.group_after.as_deref(), Some("group-cursor-1"));
    assert_eq!(normalized.entry_page_size, Some(100));
    assert_eq!(
        normalized.entry_after.as_deref(),
        Some("entry-cursor-bundle-1")
    );
    assert_eq!(normalized.read_class, Some(ReadClass::Materialized));
}

#[test]
fn normalize_api_params_decodes_path_b64() {
    let params = ApiParams {
        path: None,
        path_b64: Some(B64URL.encode(b"/bad/\xffname")),
        group: None,
        recursive: None,
        max_depth: None,
        pit_id: None,
        group_order: None,
        group_page_size: None,
        group_after: None,
        entry_page_size: None,
        entry_after: None,
        read_class: None,
    };
    let normalized = normalize_api_params(params).expect("normalize path_b64");
    assert_eq!(normalized.path, b"/bad/\xffname".to_vec());
}

#[test]
fn normalize_api_params_rejects_path_and_path_b64_together() {
    let params = ApiParams {
        path: Some("/mnt".into()),
        path_b64: Some(B64URL.encode(b"/mnt")),
        group: None,
        recursive: None,
        max_depth: None,
        pit_id: None,
        group_order: None,
        group_page_size: None,
        group_after: None,
        entry_page_size: None,
        entry_after: None,
        read_class: None,
    };
    let err = normalize_api_params(params).expect_err("reject mixed path inputs");
    assert!(err.to_string().contains("mutually exclusive"));
}

#[test]
fn tree_json_includes_bytes_safe_fields() {
    let root = TreePageRoot {
        path: b"/bad/\xffname".to_vec(),
        size: 1,
        modified_time_us: 2,
        is_dir: false,
        exists: true,
        has_children: false,
    };
    let entry = TreePageEntry {
        path: b"/bad/\xffname".to_vec(),
        depth: 1,
        size: 1,
        modified_time_us: 2,
        is_dir: false,
        has_children: false,
    };

    let root_json = tree_root_json(&root);
    assert_eq!(
        root_json["path_b64"],
        serde_json::json!(B64URL.encode(b"/bad/\xffname"))
    );
    assert_eq!(
        root_json["path"],
        serde_json::json!(path_to_string_lossy(b"/bad/\xffname"))
    );

    let entry_json = tree_entry_json(&entry);
    assert_eq!(
        entry_json["path_b64"],
        serde_json::json!(B64URL.encode(b"/bad/\xffname"))
    );
}

#[test]
fn tree_json_omits_bytes_safe_fields_for_utf8_names() {
    let root = TreePageRoot {
        path: b"/utf8/hello.txt".to_vec(),
        size: 1,
        modified_time_us: 2,
        is_dir: false,
        exists: true,
        has_children: false,
    };
    let entry = TreePageEntry {
        path: b"/utf8/hello.txt".to_vec(),
        depth: 1,
        size: 1,
        modified_time_us: 2,
        is_dir: false,
        has_children: false,
    };

    let root_json = tree_root_json(&root);
    assert!(root_json.get("path_b64").is_none());

    let entry_json = tree_entry_json(&entry);
    assert!(entry_json.get("path_b64").is_none());
}

#[test]
fn decode_stats_groups_keeps_decode_error_group() {
    let ok_payload = rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(SubtreeStats {
        total_files: 1,
        ..SubtreeStats::default()
    }))
    .expect("encode stats");
    let events = vec![
        mk_event("n1", ok_payload),
        mk_event("n2", b"bad-msgpack".to_vec()),
    ];
    let groups = decode_stats_groups(events, &origin_policy(), None, ReadClass::Materialized);
    assert_eq!(groups.len(), 2);
    assert_eq!(groups["n1"]["status"], "ok");
    assert_eq!(groups["n2"]["status"], "error");
}

#[test]
fn decode_stats_groups_materialized_zeroes_blind_spot_count() {
    let payload = rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(SubtreeStats {
        total_nodes: 5,
        blind_spot_count: 5,
        ..SubtreeStats::default()
    }))
    .expect("encode stats");

    let materialized = decode_stats_groups(
        vec![mk_event("nfs2", payload.clone())],
        &origin_policy(),
        None,
        ReadClass::Materialized,
    );
    assert_eq!(materialized["nfs2"]["data"]["blind_spot_count"], 0);

    let trusted = decode_stats_groups(
        vec![mk_event("nfs2", payload)],
        &origin_policy(),
        None,
        ReadClass::TrustedMaterialized,
    );
    assert_eq!(trusted["nfs2"]["data"]["blind_spot_count"], 0);
}

#[test]
fn decode_stats_groups_preserves_utf8_group_keys_exactly() {
    let composed = "café-👩🏽‍💻";
    let decomposed = "cafe\u{301}-👩🏽‍💻";
    let mk_stats_event = |origin: &str| -> Event {
        let payload = rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(SubtreeStats {
            total_files: 1,
            ..SubtreeStats::default()
        }))
        .expect("encode stats");
        mk_event(origin, payload)
    };

    let groups = decode_stats_groups(
        vec![mk_stats_event(composed), mk_stats_event(decomposed)],
        &origin_policy(),
        None,
        ReadClass::Materialized,
    );

    assert!(groups.contains_key(composed));
    assert!(groups.contains_key(decomposed));
    assert_ne!(composed, decomposed);
    assert_eq!(groups[composed]["status"], "ok");
    assert_eq!(groups[decomposed]["status"], "ok");
}

#[test]
fn materialized_query_readiness_waits_for_initial_audit_completion() {
    let source_status = SourceStatusSnapshot {
        current_stream_generation: None,
        logical_roots: Vec::new(),
        concrete_roots: vec![crate::source::SourceConcreteRootHealthSnapshot {
            root_key: "root-a-key".into(),
            logical_root_id: "root-a".into(),
            object_ref: "obj-a".into(),
            status: "ok".into(),
            coverage_mode: "audit_only".into(),
            watch_enabled: false,
            scan_enabled: true,
            is_group_primary: true,
            active: true,
            watch_lru_capacity: 0,
            audit_interval_ms: 10_000,
            overflow_count: 0,
            overflow_pending: false,
            rescan_pending: false,
            last_rescan_reason: None,
            last_error: None,
            last_audit_started_at_us: Some(10),
            last_audit_completed_at_us: Some(20),
            last_audit_duration_ms: Some(1),
            emitted_batch_count: 0,
            emitted_event_count: 0,
            emitted_control_event_count: 0,
            emitted_data_event_count: 0,
            emitted_path_capture_target: None,
            emitted_path_event_count: 0,
            last_emitted_at_us: None,
            last_emitted_origins: Vec::new(),
            forwarded_batch_count: 0,
            forwarded_event_count: 0,
            forwarded_path_event_count: 0,
            last_forwarded_at_us: None,
            last_forwarded_origins: Vec::new(),
            current_revision: None,
            current_stream_generation: None,
            candidate_revision: None,
            candidate_stream_generation: None,
            candidate_status: None,
            draining_revision: None,
            draining_stream_generation: None,
            draining_status: None,
        }],
        degraded_roots: Vec::new(),
    };
    let sink_status = SinkStatusSnapshot {
        live_nodes: 0,
        tombstoned_count: 0,
        attested_count: 0,
        suspect_count: 0,
        blind_spot_count: 0,
        shadow_time_us: 0,
        estimated_heap_bytes: 0,
        scheduled_groups_by_node: BTreeMap::new(),
        last_control_frame_signals_by_node: BTreeMap::new(),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "root-a".into(),
            primary_object_ref: "obj-a".into(),
            total_nodes: 0,
            live_nodes: 0,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 0,
            shadow_lag_us: 0,
            overflow_pending_audit: false,
            initial_audit_completed: false,
            readiness: crate::sink::GroupReadinessState::PendingAudit,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };

    let err = materialized_query_readiness_error(&source_status, &sink_status)
        .expect("initial audit should gate materialized queries");
    assert!(err.contains("initial audit incomplete"));
    assert!(err.contains("root-a"));
}

#[test]
fn cached_sink_status_drops_cached_ready_group_when_fresh_snapshot_explicitly_reports_same_group_empty(
) {
    let source_status = SourceStatusSnapshot {
        current_stream_generation: Some(1),
        logical_roots: vec![crate::source::SourceLogicalRootHealthSnapshot {
            root_id: "nfs1".into(),
            status: "ok".into(),
            matched_grants: 1,
            active_members: 1,
            coverage_mode: "realtime_hotset_plus_audit".into(),
        }],
        concrete_roots: vec![crate::source::SourceConcreteRootHealthSnapshot {
            root_key: "nfs1-key".into(),
            logical_root_id: "nfs1".into(),
            object_ref: "node-a::nfs1".into(),
            status: "ok".into(),
            coverage_mode: "realtime_hotset_plus_audit".into(),
            watch_enabled: true,
            scan_enabled: true,
            is_group_primary: true,
            active: true,
            watch_lru_capacity: 128,
            audit_interval_ms: 10_000,
            overflow_count: 0,
            overflow_pending: false,
            rescan_pending: false,
            last_rescan_reason: None,
            last_error: None,
            last_audit_started_at_us: None,
            last_audit_completed_at_us: None,
            last_audit_duration_ms: None,
            emitted_batch_count: 0,
            emitted_event_count: 0,
            emitted_control_event_count: 0,
            emitted_data_event_count: 0,
            emitted_path_capture_target: None,
            emitted_path_event_count: 0,
            last_emitted_at_us: None,
            last_emitted_origins: Vec::new(),
            forwarded_batch_count: 0,
            forwarded_event_count: 0,
            forwarded_path_event_count: 0,
            last_forwarded_at_us: None,
            last_forwarded_origins: Vec::new(),
            current_revision: None,
            current_stream_generation: None,
            candidate_revision: None,
            candidate_stream_generation: None,
            candidate_status: None,
            draining_revision: None,
            draining_stream_generation: None,
            draining_status: None,
        }],
        degraded_roots: Vec::new(),
    };
    let cached = CachedSinkStatusSnapshot {
        snapshot: SinkStatusSnapshot {
            groups: vec![sink_group_status("nfs1", true)],
            scheduled_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            ..SinkStatusSnapshot::default()
        },
    };
    let allowed_groups = BTreeSet::from(["nfs1".to_string()]);
    let fresh = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs1".to_string(),
            primary_object_ref: "nfs1-owner".to_string(),
            total_nodes: 0,
            live_nodes: 0,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 0,
            shadow_lag_us: 0,
            overflow_pending_audit: false,
            initial_audit_completed: false,
            readiness: crate::sink::GroupReadinessState::PendingAudit,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        scheduled_groups_by_node: BTreeMap::new(),
        ..SinkStatusSnapshot::default()
    };

    let merged = merge_with_cached_sink_status_snapshot(Some(&cached), &allowed_groups, fresh);
    let merged_groups = merged
        .groups
        .iter()
        .map(|group| {
            (
                group.group_id.as_str(),
                group.initial_audit_completed,
                group.total_nodes,
                group.live_nodes,
                group.shadow_time_us,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(merged_groups, vec![("nfs1", false, 0, 0, 0)]);
    assert!(
        materialized_query_readiness_error(&source_status, &merged).is_some(),
        "fresh explicit empty same-group sink snapshot must clear cached trusted-materialized readiness instead of preserving stale ready state"
    );
}

#[test]
fn materialized_query_readiness_fail_closed_when_sink_group_missing() {
    let source_status = SourceStatusSnapshot {
        current_stream_generation: None,
        logical_roots: vec![crate::source::SourceLogicalRootHealthSnapshot {
            root_id: "root-a".into(),
            status: "ok".into(),
            matched_grants: 1,
            active_members: 1,
            coverage_mode: "audit_only".into(),
        }],
        concrete_roots: Vec::new(),
        degraded_roots: Vec::new(),
    };
    let sink_status = SinkStatusSnapshot::default();

    let err = materialized_query_readiness_error(&source_status, &sink_status)
        .expect("missing sink group must gate materialized queries");
    assert!(err.contains("initial audit incomplete"));
    assert!(err.contains("root-a"));
}

#[test]
fn materialized_query_readiness_ignores_inactive_or_non_scan_groups() {
    let source_status = SourceStatusSnapshot {
        current_stream_generation: None,
        logical_roots: Vec::new(),
        concrete_roots: vec![
            crate::source::SourceConcreteRootHealthSnapshot {
                root_key: "root-a-key".into(),
                logical_root_id: "root-a".into(),
                object_ref: "obj-a".into(),
                status: "ok".into(),
                coverage_mode: "watch_only".into(),
                watch_enabled: true,
                scan_enabled: false,
                is_group_primary: true,
                active: true,
                watch_lru_capacity: 128,
                audit_interval_ms: 10_000,
                overflow_count: 0,
                overflow_pending: false,
                rescan_pending: false,
                last_rescan_reason: None,
                last_error: None,
                last_audit_started_at_us: None,
                last_audit_completed_at_us: None,
                last_audit_duration_ms: None,
                emitted_batch_count: 0,
                emitted_event_count: 0,
                emitted_control_event_count: 0,
                emitted_data_event_count: 0,
                emitted_path_capture_target: None,
                emitted_path_event_count: 0,
                last_emitted_at_us: None,
                last_emitted_origins: Vec::new(),
                forwarded_batch_count: 0,
                forwarded_event_count: 0,
                forwarded_path_event_count: 0,
                last_forwarded_at_us: None,
                last_forwarded_origins: Vec::new(),
                current_revision: None,
                current_stream_generation: None,
                candidate_revision: None,
                candidate_stream_generation: None,
                candidate_status: None,
                draining_revision: None,
                draining_stream_generation: None,
                draining_status: None,
            },
            crate::source::SourceConcreteRootHealthSnapshot {
                root_key: "root-b-key".into(),
                logical_root_id: "root-b".into(),
                object_ref: "obj-b".into(),
                status: "ok".into(),
                coverage_mode: "audit_only".into(),
                watch_enabled: false,
                scan_enabled: true,
                is_group_primary: true,
                active: false,
                watch_lru_capacity: 0,
                audit_interval_ms: 10_000,
                overflow_count: 0,
                overflow_pending: false,
                rescan_pending: false,
                last_rescan_reason: None,
                last_error: None,
                last_audit_started_at_us: None,
                last_audit_completed_at_us: None,
                last_audit_duration_ms: None,
                emitted_batch_count: 0,
                emitted_event_count: 0,
                emitted_control_event_count: 0,
                emitted_data_event_count: 0,
                emitted_path_capture_target: None,
                emitted_path_event_count: 0,
                last_emitted_at_us: None,
                last_emitted_origins: Vec::new(),
                forwarded_batch_count: 0,
                forwarded_event_count: 0,
                forwarded_path_event_count: 0,
                last_forwarded_at_us: None,
                last_forwarded_origins: Vec::new(),
                current_revision: None,
                current_stream_generation: None,
                candidate_revision: None,
                candidate_stream_generation: None,
                candidate_status: None,
                draining_revision: None,
                draining_stream_generation: None,
                draining_status: None,
            },
        ],
        degraded_roots: Vec::new(),
    };
    let sink_status = SinkStatusSnapshot::default();
    assert!(materialized_query_readiness_error(&source_status, &sink_status).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_retries_empty_first_ranked_group_for_non_root_subtree_once(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    let root_c = tmp.path().join("node-b-nfs3");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    fs::create_dir_all(&root_c).expect("create node-b nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs3".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_c,
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-trusted-subtree-ready-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-trusted-subtree-proxy-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .filter_map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params.scope.selected_group?;
                    Some(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    ))
                })
                .collect::<Vec<_>>()
        },
    );
    let owner_a_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let owner_a_calls_for_handler = owner_a_calls.clone();
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-subtree-owner-a-endpoint",
        CancellationToken::new(),
        move |requests| {
            let owner_a_calls = owner_a_calls_for_handler.clone();
            async move {
                let mut responses = Vec::new();
                for req in requests {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-a query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-a request");
                    let nth = owner_a_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let payload = if group_id == "nfs1" && nth == 0 {
                        empty_materialized_tree_payload_for_test(&params.scope.path)
                    } else {
                        real_materialized_tree_payload_for_test(&params.scope.path)
                    };
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-a request correlation"),
                        payload,
                    ));
                }
                responses
            }
        },
    );
    let owner_b_route = sink_query_request_route_for("node-b");
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_b_route.clone(),
        "test-trusted-subtree-owner-b-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-b query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-b request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-b request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/data".to_vec(),
        group: None,
        recursive: false,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(3),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };

    let session = tokio::time::timeout(
        Duration::from_secs(6),
        build_tree_pit_session(
            &state,
            &ProjectionPolicy::default(),
            &params,
            Duration::from_millis(3400),
            ObservationStatus::trusted_materialized(),
            Some(&request_sink_status),
        ),
    )
    .await
    .expect("trusted non-root subtree retry should settle within the PIT budget")
    .expect("build tree pit session");

    assert_eq!(session.groups.len(), 3);
    assert!(
        session.groups.iter().all(|group| {
            group
                .root
                .as_ref()
                .is_some_and(|root| root.exists && root.path == b"/data")
        }),
        "trusted-ready non-root subtree should retry a transient empty first-ranked owner result instead of settling an empty root: {:?}",
        session
            .groups
            .iter()
            .map(|group| {
                (
                    group.group.clone(),
                    group
                        .root
                        .as_ref()
                        .map(|root| (root.exists, root.path.clone())),
                )
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(
        owner_a_calls.load(std::sync::atomic::Ordering::SeqCst),
        3,
        "first-ranked non-root trusted subtree should retry the owner once before accepting later groups"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_fail_closes_when_first_ranked_non_root_subtree_owner_and_proxy_stay_empty(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    let root_c = tmp.path().join("node-b-nfs3");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    fs::create_dir_all(&root_c).expect("create node-b nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs3".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_c,
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-trusted-subtree-persistent-empty-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-trusted-subtree-persistent-empty-proxy-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .filter_map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params.scope.selected_group?;
                    let payload = if group_id == "nfs1" {
                        empty_materialized_tree_payload_for_test(&params.scope.path)
                    } else {
                        real_materialized_tree_payload_for_test(&params.scope.path)
                    };
                    Some(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation"),
                        payload,
                    ))
                })
                .collect::<Vec<_>>()
        },
    );
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-subtree-persistent-empty-owner-a-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode owner-a query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for owner-a request");
                let payload = if group_id == "nfs1" {
                    empty_materialized_tree_payload_for_test(&params.scope.path)
                } else {
                    real_materialized_tree_payload_for_test(&params.scope.path)
                };
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("owner-a request correlation"),
                    payload,
                ));
            }
            responses
        },
    );
    let owner_b_route = sink_query_request_route_for("node-b");
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_b_route.clone(),
        "test-trusted-subtree-persistent-empty-owner-b-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-b query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-b request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-b request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/data".to_vec(),
        group: None,
        recursive: false,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(3),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };

    let err = tokio::time::timeout(
        Duration::from_secs(6),
        build_tree_pit_session(
            &state,
            &ProjectionPolicy::default(),
            &params,
            Duration::from_millis(3400),
            ObservationStatus::trusted_materialized(),
            Some(&request_sink_status),
        ),
    )
    .await
    .expect("trusted non-root subtree persistent-empty path should settle within the PIT budget")
    .expect_err(
        "trusted-ready first-ranked non-root subtree must fail closed instead of settling an ok empty root when owner and proxy stay empty",
    );

    assert!(
        matches!(err, CnxError::NotReady(ref msg) if msg.contains("nfs1")),
        "trusted-ready first-ranked non-root subtree persistent-empty failure should stay localized to nfs1: {err:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_retries_empty_first_ranked_group_for_non_root_subtree_with_max_depth_once(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    let root_c = tmp.path().join("node-b-nfs3");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    fs::create_dir_all(&root_c).expect("create node-b nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs3".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_c,
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-trusted-subtree-max-depth-ready-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-trusted-subtree-max-depth-proxy-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .filter_map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params.scope.selected_group?;
                    let payload = if group_id == "nfs1" {
                        real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/nested/child"],
                        )
                    } else if group_id == "nfs2" {
                        real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/nested/peer.txt"],
                        )
                    } else {
                        empty_materialized_tree_payload_for_test(&params.scope.path)
                    };
                    Some(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation"),
                        payload,
                    ))
                })
                .collect::<Vec<_>>()
        },
    );
    let owner_a_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let owner_a_calls_for_handler = owner_a_calls.clone();
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-subtree-max-depth-owner-a-endpoint",
        CancellationToken::new(),
        move |requests| {
            let owner_a_calls = owner_a_calls_for_handler.clone();
            async move {
                let mut responses = Vec::new();
                for req in requests {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-a query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-a request");
                    let nth = owner_a_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let payload = if group_id == "nfs1" && nth == 0 {
                        empty_materialized_tree_payload_for_test(&params.scope.path)
                    } else if group_id == "nfs1" {
                        real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/nested/child"],
                        )
                    } else {
                        real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/nested/peer.txt"],
                        )
                    };
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-a request correlation"),
                        payload,
                    ));
                }
                responses
            }
        },
    );
    let owner_b_route = sink_query_request_route_for("node-b");
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_b_route.clone(),
        "test-trusted-subtree-max-depth-owner-b-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-b query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-b request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-b request correlation"),
                        empty_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/nested".to_vec(),
        group: None,
        recursive: true,
        max_depth: Some(1),
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(3),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };

    let session = tokio::time::timeout(
        Duration::from_secs(6),
        build_tree_pit_session(
            &state,
            &ProjectionPolicy::default(),
            &params,
            Duration::from_millis(3400),
            ObservationStatus::trusted_materialized(),
            Some(&request_sink_status),
        ),
    )
    .await
    .expect("trusted non-root max-depth subtree retry should settle within the PIT budget")
    .expect("build tree pit session");

    assert_eq!(session.groups.len(), 3);
    let group_roots = session
        .groups
        .iter()
        .map(|group| {
            (
                group.group.clone(),
                group
                    .root
                    .as_ref()
                    .map(|root| (root.exists, root.path.clone(), group.entries.len())),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        group_roots,
        vec![
            ("nfs1".to_string(), Some((true, b"/nested".to_vec(), 1))),
            ("nfs2".to_string(), Some((true, b"/nested".to_vec(), 1))),
            ("nfs3".to_string(), Some((false, b"/nested".to_vec(), 0))),
        ],
        "trusted-ready non-root max-depth subtree should retry a transient empty first-ranked owner result instead of settling an empty root: {group_roots:?}"
    );
    assert_eq!(
        owner_a_calls.load(std::sync::atomic::Ordering::SeqCst),
        3,
        "first-ranked max-depth trusted subtree should retry the owner once before accepting later groups"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_falls_back_to_proxy_when_first_ranked_non_root_subtree_retry_times_out(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    let root_c = tmp.path().join("node-b-nfs3");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    fs::create_dir_all(&root_c).expect("create node-b nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs3".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_c,
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-trusted-subtree-ready-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-trusted-subtree-proxy-fallback-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .filter_map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params.scope.selected_group?;
                    Some(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    ))
                })
                .collect::<Vec<_>>()
        },
    );
    let owner_a_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let owner_a_calls_for_handler = owner_a_calls.clone();
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-subtree-owner-a-timeout-endpoint",
        CancellationToken::new(),
        move |requests| {
            let owner_a_calls = owner_a_calls_for_handler.clone();
            async move {
                let mut responses = Vec::new();
                for req in requests {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-a query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-a request");
                    let nth = if group_id == "nfs1" {
                        owner_a_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    } else {
                        usize::MAX
                    };
                    if group_id == "nfs1" && nth == 0 {
                        responses.push(mk_event_with_correlation(
                            &group_id,
                            req.metadata()
                                .correlation_id
                                .expect("owner-a request correlation"),
                            empty_materialized_tree_payload_for_test(&params.scope.path),
                        ));
                        continue;
                    }
                    if group_id == "nfs1" && nth == 1 {
                        tokio::time::sleep(Duration::from_millis(700)).await;
                        continue;
                    }
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-a request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    ));
                }
                responses
            }
        },
    );
    let owner_b_route = sink_query_request_route_for("node-b");
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_b_route.clone(),
        "test-trusted-subtree-owner-b-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-b query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-b request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-b request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/data".to_vec(),
        group: None,
        recursive: false,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(3),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };

    let session = tokio::time::timeout(
        Duration::from_secs(6),
        build_tree_pit_session(
            &state,
            &ProjectionPolicy::default(),
            &params,
            Duration::from_millis(3400),
            ObservationStatus::trusted_materialized(),
            Some(&request_sink_status),
        ),
    )
    .await
    .expect("trusted non-root subtree timeout fallback should settle within the PIT budget")
    .expect("build tree pit session");

    assert_eq!(session.groups.len(), 3);
    assert!(
        session.groups.iter().all(|group| {
            group
                .root
                .as_ref()
                .is_some_and(|root| root.exists && root.path == b"/data")
        }),
        "trusted-ready non-root subtree should fall back to generic proxy when the owner retry times out after an initial empty result: {:?}",
        session
            .groups
            .iter()
            .map(|group| {
                (
                    group.group.clone(),
                    group
                        .root
                        .as_ref()
                        .map(|root| (root.exists, root.path.clone())),
                )
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(
        owner_a_calls.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "first-ranked non-root trusted subtree should try the owner twice before falling back to proxy"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_preserves_richer_first_ranked_non_root_max_depth_proxy_payload_after_owner_retry_timeout(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    let root_c = tmp.path().join("node-b-nfs3");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    fs::create_dir_all(&root_c).expect("create node-b nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs3".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_c,
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-trusted-first-ranked-non-root-max-depth-proxy-rich-ready-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-trusted-first-ranked-non-root-max-depth-proxy-rich-then-empty-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode proxy query request");
                let Some(group_id) = params.scope.selected_group.clone() else {
                    continue;
                };
                let correlation = req
                    .metadata()
                    .correlation_id
                    .expect("proxy request correlation");
                if group_id == "nfs1" {
                    responses.push(Event::new(
                        EventMetadata {
                            origin_id: NodeId(group_id.clone()),
                            timestamp_us: 1,
                            logical_ts: None,
                            correlation_id: Some(correlation),
                            ingress_auth: None,
                            trace: None,
                        },
                        Bytes::from(real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/nested/child"],
                        )),
                    ));
                    responses.push(Event::new(
                        EventMetadata {
                            origin_id: NodeId(group_id.clone()),
                            timestamp_us: 2,
                            logical_ts: None,
                            correlation_id: Some(correlation),
                            ingress_auth: None,
                            trace: None,
                        },
                        Bytes::from(empty_materialized_tree_payload_for_test(&params.scope.path)),
                    ));
                } else if group_id == "nfs2" {
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        correlation,
                        real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/nested/peer.txt"],
                        ),
                    ));
                } else {
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        correlation,
                        empty_materialized_tree_payload_for_test(&params.scope.path),
                    ));
                }
            }
            responses
        },
    );
    let owner_a_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let owner_a_calls_for_handler = owner_a_calls.clone();
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-first-ranked-non-root-max-depth-owner-a-timeout-endpoint",
        CancellationToken::new(),
        move |requests| {
            let owner_a_calls = owner_a_calls_for_handler.clone();
            async move {
                let mut responses = Vec::new();
                for req in requests {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-a query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-a request");
                    let nth = if group_id == "nfs1" {
                        owner_a_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    } else {
                        usize::MAX
                    };
                    if group_id == "nfs1" && nth == 0 {
                        responses.push(mk_event_with_correlation(
                            &group_id,
                            req.metadata()
                                .correlation_id
                                .expect("owner-a request correlation"),
                            empty_materialized_tree_payload_for_test(&params.scope.path),
                        ));
                        continue;
                    }
                    if group_id == "nfs1" && nth == 1 {
                        tokio::time::sleep(Duration::from_millis(700)).await;
                        continue;
                    }
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-a request correlation"),
                        real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/nested/peer.txt"],
                        ),
                    ));
                }
                responses
            }
        },
    );
    let owner_b_route = sink_query_request_route_for("node-b");
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_b_route.clone(),
        "test-trusted-first-ranked-non-root-max-depth-owner-b-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-b query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-b request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-b request correlation"),
                        empty_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/nested".to_vec(),
        group: None,
        recursive: true,
        max_depth: Some(1),
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(3),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };

    let session = tokio::time::timeout(
            Duration::from_secs(6),
            build_tree_pit_session(
                &state,
                &ProjectionPolicy::default(),
                &params,
                Duration::from_millis(3400),
                ObservationStatus::trusted_materialized(),
                Some(&request_sink_status),
            ),
        )
        .await
        .expect("trusted first-ranked non-root max-depth proxy rich fallback should settle within the PIT budget")
        .expect("build tree pit session");

    assert_eq!(session.groups.len(), 3);
    let group_roots = session
        .groups
        .iter()
        .map(|group| {
            (
                group.group.clone(),
                group
                    .root
                    .as_ref()
                    .map(|root| (root.exists, root.path.clone(), group.entries.len())),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        group_roots,
        vec![
            ("nfs1".to_string(), Some((true, b"/nested".to_vec(), 1))),
            ("nfs2".to_string(), Some((true, b"/nested".to_vec(), 1))),
            ("nfs3".to_string(), Some((false, b"/nested".to_vec(), 0))),
        ],
        "trusted first-ranked non-root max-depth proxy fallback must preserve a richer same-batch payload instead of letting a later empty root overwrite it after owner retry timeout: {group_roots:?}"
    );
    assert_eq!(
        owner_a_calls.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "first-ranked max-depth trusted subtree should try the owner twice before falling back to proxy"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_falls_back_to_proxy_when_middle_ranked_non_root_max_depth_owner_remains_empty(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    let root_c = tmp.path().join("node-b-nfs3");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    fs::create_dir_all(&root_c).expect("create node-b nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs3".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_c,
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-trusted-middle-ranked-non-root-max-depth-ready-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-trusted-middle-ranked-non-root-max-depth-proxy-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .filter_map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params.scope.selected_group?;
                    let payload = if group_id == "nfs1" {
                        real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/nested/child"],
                        )
                    } else if group_id == "nfs2" {
                        real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/nested/peer.txt"],
                        )
                    } else {
                        empty_materialized_tree_payload_for_test(&params.scope.path)
                    };
                    Some(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation"),
                        payload,
                    ))
                })
                .collect::<Vec<_>>()
        },
    );
    let owner_a_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let owner_a_calls_for_handler = owner_a_calls.clone();
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-middle-ranked-non-root-max-depth-owner-a-empty-endpoint",
        CancellationToken::new(),
        move |requests| {
            let owner_a_calls = owner_a_calls_for_handler.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        let params =
                            rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                                .expect("decode owner-a query request");
                        let group_id = params
                            .scope
                            .selected_group
                            .clone()
                            .expect("selected group for owner-a request");
                        let payload = if group_id == "nfs1" {
                            real_materialized_tree_payload_with_entries_for_test(
                                &params.scope.path,
                                &[b"/nested/child"],
                            )
                        } else if group_id == "nfs2" {
                            owner_a_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            empty_materialized_tree_payload_for_test(&params.scope.path)
                        } else {
                            real_materialized_tree_payload_with_entries_for_test(
                                &params.scope.path,
                                &[b"/nested/peer.txt"],
                            )
                        };
                        mk_event_with_correlation(
                            &group_id,
                            req.metadata()
                                .correlation_id
                                .expect("owner-a request correlation"),
                            payload,
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let owner_b_route = sink_query_request_route_for("node-b");
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_b_route.clone(),
        "test-trusted-middle-ranked-non-root-max-depth-owner-b-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-b query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-b request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-b request correlation"),
                        empty_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/nested".to_vec(),
        group: None,
        recursive: true,
        max_depth: Some(1),
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(3),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };

    let session = tokio::time::timeout(
            Duration::from_secs(6),
            build_tree_pit_session(
                &state,
                &ProjectionPolicy::default(),
                &params,
                Duration::from_millis(3400),
                ObservationStatus::trusted_materialized(),
                Some(&request_sink_status),
            ),
        )
        .await
        .expect("trusted middle-ranked non-root max-depth proxy fallback should settle within the PIT budget")
        .expect("build tree pit session");

    assert_eq!(session.groups.len(), 3);
    let group_roots = session
        .groups
        .iter()
        .map(|group| {
            (
                group.group.clone(),
                group
                    .root
                    .as_ref()
                    .map(|root| (root.exists, root.path.clone(), group.entries.len())),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        group_roots,
        vec![
            ("nfs1".to_string(), Some((true, b"/nested".to_vec(), 1))),
            ("nfs2".to_string(), Some((true, b"/nested".to_vec(), 1))),
            ("nfs3".to_string(), Some((false, b"/nested".to_vec(), 0))),
        ],
        "trusted middle-ranked non-root max-depth proxy fallback must preserve the richer nfs2 subtree instead of settling an ok empty root after the owner stays empty: {group_roots:?}"
    );
    assert_eq!(
        owner_a_calls.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "middle-ranked non-root max-depth trusted subtree should try the owner twice before falling back to proxy"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[test]
fn materialized_query_readiness_ignores_retired_logical_root_without_active_scan_primary() {
    let source_status = SourceStatusSnapshot {
        current_stream_generation: Some(1),
        logical_roots: vec![
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "nfs2".into(),
                status: "ok".into(),
                matched_grants: 3,
                active_members: 3,
                coverage_mode: "realtime_hotset_plus_audit".into(),
            },
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "nfs4".into(),
                status: "ok".into(),
                matched_grants: 3,
                active_members: 3,
                coverage_mode: "realtime_hotset_plus_audit".into(),
            },
        ],
        concrete_roots: vec![crate::source::SourceConcreteRootHealthSnapshot {
            root_key: "nfs2-key".into(),
            logical_root_id: "nfs2".into(),
            object_ref: "node-a::nfs2".into(),
            status: "running".into(),
            coverage_mode: "realtime_hotset_plus_audit".into(),
            watch_enabled: true,
            scan_enabled: true,
            is_group_primary: true,
            active: true,
            watch_lru_capacity: 128,
            audit_interval_ms: 10_000,
            overflow_count: 0,
            overflow_pending: false,
            rescan_pending: false,
            last_rescan_reason: Some("manual".into()),
            last_error: None,
            last_audit_started_at_us: Some(10),
            last_audit_completed_at_us: Some(20),
            last_audit_duration_ms: Some(1),
            emitted_batch_count: 1,
            emitted_event_count: 1,
            emitted_control_event_count: 0,
            emitted_data_event_count: 1,
            emitted_path_capture_target: None,
            emitted_path_event_count: 0,
            last_emitted_at_us: Some(30),
            last_emitted_origins: Vec::new(),
            forwarded_batch_count: 1,
            forwarded_event_count: 1,
            forwarded_path_event_count: 0,
            last_forwarded_at_us: Some(30),
            last_forwarded_origins: Vec::new(),
            current_revision: Some(1),
            current_stream_generation: Some(1),
            candidate_revision: None,
            candidate_stream_generation: None,
            candidate_status: None,
            draining_revision: None,
            draining_stream_generation: None,
            draining_status: None,
        }],
        degraded_roots: Vec::new(),
    };
    let sink_status = SinkStatusSnapshot {
        groups: vec![sink_group_status("nfs2", true)],
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs2".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };

    assert!(
        materialized_query_readiness_error(&source_status, &sink_status).is_none(),
        "a stale logical root without any active scan-enabled primary concrete root must not keep trusted-materialized readiness blocked after contraction to nfs2"
    );
}

#[test]
fn filter_source_status_snapshot_drops_groups_outside_current_roots() {
    let snapshot = SourceStatusSnapshot {
        current_stream_generation: None,
        logical_roots: vec![
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "root-a".into(),
                status: "ok".into(),
                matched_grants: 1,
                active_members: 1,
                coverage_mode: "audit_only".into(),
            },
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "root-b".into(),
                status: "ok".into(),
                matched_grants: 1,
                active_members: 1,
                coverage_mode: "audit_only".into(),
            },
        ],
        concrete_roots: vec![
            crate::source::SourceConcreteRootHealthSnapshot {
                root_key: "root-a-key".into(),
                logical_root_id: "root-a".into(),
                object_ref: "obj-a".into(),
                status: "ok".into(),
                coverage_mode: "audit_only".into(),
                watch_enabled: false,
                scan_enabled: true,
                is_group_primary: true,
                active: true,
                watch_lru_capacity: 0,
                audit_interval_ms: 10_000,
                overflow_count: 0,
                overflow_pending: false,
                rescan_pending: false,
                last_rescan_reason: None,
                last_error: None,
                last_audit_started_at_us: None,
                last_audit_completed_at_us: None,
                last_audit_duration_ms: None,
                emitted_batch_count: 0,
                emitted_event_count: 0,
                emitted_control_event_count: 0,
                emitted_data_event_count: 0,
                emitted_path_capture_target: None,
                emitted_path_event_count: 0,
                last_emitted_at_us: None,
                last_emitted_origins: Vec::new(),
                forwarded_batch_count: 0,
                forwarded_event_count: 0,
                forwarded_path_event_count: 0,
                last_forwarded_at_us: None,
                last_forwarded_origins: Vec::new(),
                current_revision: None,
                current_stream_generation: None,
                candidate_revision: None,
                candidate_stream_generation: None,
                candidate_status: None,
                draining_revision: None,
                draining_stream_generation: None,
                draining_status: None,
            },
            crate::source::SourceConcreteRootHealthSnapshot {
                root_key: "root-b-key".into(),
                logical_root_id: "root-b".into(),
                object_ref: "obj-b".into(),
                status: "ok".into(),
                coverage_mode: "audit_only".into(),
                watch_enabled: false,
                scan_enabled: true,
                is_group_primary: true,
                active: true,
                watch_lru_capacity: 0,
                audit_interval_ms: 10_000,
                overflow_count: 0,
                overflow_pending: false,
                rescan_pending: false,
                last_rescan_reason: None,
                last_error: None,
                last_audit_started_at_us: None,
                last_audit_completed_at_us: None,
                last_audit_duration_ms: None,
                emitted_batch_count: 0,
                emitted_event_count: 0,
                emitted_control_event_count: 0,
                emitted_data_event_count: 0,
                emitted_path_capture_target: None,
                emitted_path_event_count: 0,
                last_emitted_at_us: None,
                last_emitted_origins: Vec::new(),
                forwarded_batch_count: 0,
                forwarded_event_count: 0,
                forwarded_path_event_count: 0,
                last_forwarded_at_us: None,
                last_forwarded_origins: Vec::new(),
                current_revision: None,
                current_stream_generation: None,
                candidate_revision: None,
                candidate_stream_generation: None,
                candidate_status: None,
                draining_revision: None,
                draining_stream_generation: None,
                draining_status: None,
            },
        ],
        degraded_roots: vec![
            ("root-a".into(), "degraded".into()),
            ("root-b".into(), "degraded".into()),
        ],
    };
    let filtered = filter_source_status_snapshot(snapshot, &BTreeSet::from(["root-b".to_string()]));
    assert_eq!(filtered.logical_roots.len(), 1);
    assert_eq!(filtered.logical_roots[0].root_id, "root-b");
    assert_eq!(filtered.concrete_roots.len(), 1);
    assert_eq!(filtered.concrete_roots[0].logical_root_id, "root-b");
    assert_eq!(
        filtered.degraded_roots,
        vec![("root-b".to_string(), "degraded".to_string())]
    );
}

#[test]
fn merge_source_status_snapshots_prefers_later_ready_truth_for_same_group() {
    let stale = SourceStatusSnapshot {
        current_stream_generation: Some(1),
        logical_roots: vec![crate::source::SourceLogicalRootHealthSnapshot {
            root_id: "root-a".into(),
            status: "output_closed".into(),
            matched_grants: 1,
            active_members: 0,
            coverage_mode: "audit_only".into(),
        }],
        concrete_roots: vec![crate::source::SourceConcreteRootHealthSnapshot {
            root_key: "root-a-key".into(),
            logical_root_id: "root-a".into(),
            object_ref: "node-a::root-a".into(),
            status: "output_closed".into(),
            coverage_mode: "audit_only".into(),
            watch_enabled: false,
            scan_enabled: true,
            is_group_primary: true,
            active: false,
            watch_lru_capacity: 0,
            audit_interval_ms: 10_000,
            overflow_count: 0,
            overflow_pending: false,
            rescan_pending: true,
            last_rescan_reason: Some("manual".into()),
            last_error: Some("bridge stopped".into()),
            last_audit_started_at_us: None,
            last_audit_completed_at_us: None,
            last_audit_duration_ms: None,
            emitted_batch_count: 0,
            emitted_event_count: 0,
            emitted_control_event_count: 0,
            emitted_data_event_count: 0,
            emitted_path_capture_target: None,
            emitted_path_event_count: 0,
            last_emitted_at_us: None,
            last_emitted_origins: Vec::new(),
            forwarded_batch_count: 0,
            forwarded_event_count: 0,
            forwarded_path_event_count: 0,
            last_forwarded_at_us: None,
            last_forwarded_origins: Vec::new(),
            current_revision: Some(1),
            current_stream_generation: Some(1),
            candidate_revision: None,
            candidate_stream_generation: None,
            candidate_status: None,
            draining_revision: None,
            draining_stream_generation: None,
            draining_status: None,
        }],
        degraded_roots: Vec::new(),
    };
    let ready = SourceStatusSnapshot {
        current_stream_generation: Some(2),
        logical_roots: vec![crate::source::SourceLogicalRootHealthSnapshot {
            root_id: "root-a".into(),
            status: "ok".into(),
            matched_grants: 2,
            active_members: 2,
            coverage_mode: "realtime_hotset_plus_audit".into(),
        }],
        concrete_roots: vec![crate::source::SourceConcreteRootHealthSnapshot {
            root_key: "root-a-key".into(),
            logical_root_id: "root-a".into(),
            object_ref: "node-a::root-a".into(),
            status: "ok".into(),
            coverage_mode: "realtime_hotset_plus_audit".into(),
            watch_enabled: true,
            scan_enabled: true,
            is_group_primary: true,
            active: true,
            watch_lru_capacity: 128,
            audit_interval_ms: 10_000,
            overflow_count: 0,
            overflow_pending: false,
            rescan_pending: false,
            last_rescan_reason: None,
            last_error: None,
            last_audit_started_at_us: Some(10),
            last_audit_completed_at_us: Some(20),
            last_audit_duration_ms: Some(1),
            emitted_batch_count: 6,
            emitted_event_count: 18,
            emitted_control_event_count: 6,
            emitted_data_event_count: 12,
            emitted_path_capture_target: None,
            emitted_path_event_count: 0,
            last_emitted_at_us: Some(30),
            last_emitted_origins: vec!["node-a".into()],
            forwarded_batch_count: 0,
            forwarded_event_count: 0,
            forwarded_path_event_count: 0,
            last_forwarded_at_us: None,
            last_forwarded_origins: Vec::new(),
            current_revision: Some(2),
            current_stream_generation: Some(2),
            candidate_revision: None,
            candidate_stream_generation: None,
            candidate_status: None,
            draining_revision: None,
            draining_stream_generation: None,
            draining_status: None,
        }],
        degraded_roots: Vec::new(),
    };

    let merged = merge_source_status_snapshots(vec![stale, ready]);

    assert_eq!(merged.current_stream_generation, Some(2));
    assert_eq!(merged.logical_roots.len(), 1);
    assert_eq!(merged.logical_roots[0].root_id, "root-a");
    assert_eq!(merged.logical_roots[0].status, "ok");
    assert_eq!(merged.logical_roots[0].matched_grants, 2);
    assert_eq!(merged.logical_roots[0].active_members, 2);
    assert_eq!(merged.concrete_roots.len(), 1);
    assert_eq!(merged.concrete_roots[0].root_key, "root-a-key");
    assert_eq!(merged.concrete_roots[0].status, "ok");
    assert!(merged.concrete_roots[0].active);
    assert!(!merged.concrete_roots[0].rescan_pending);
    assert_eq!(merged.concrete_roots[0].current_stream_generation, Some(2));
}

#[test]
fn materialized_query_readiness_ignores_stale_source_groups_outside_current_roots() {
    let source_status = SourceStatusSnapshot {
        current_stream_generation: None,
        logical_roots: vec![
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "root-a".into(),
                status: "ok".into(),
                matched_grants: 1,
                active_members: 1,
                coverage_mode: "audit_only".into(),
            },
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "root-b".into(),
                status: "ok".into(),
                matched_grants: 1,
                active_members: 1,
                coverage_mode: "audit_only".into(),
            },
        ],
        concrete_roots: vec![
            crate::source::SourceConcreteRootHealthSnapshot {
                root_key: "root-a-key".into(),
                logical_root_id: "root-a".into(),
                object_ref: "obj-a".into(),
                status: "ok".into(),
                coverage_mode: "audit_only".into(),
                watch_enabled: false,
                scan_enabled: true,
                is_group_primary: true,
                active: true,
                watch_lru_capacity: 0,
                audit_interval_ms: 10_000,
                overflow_count: 0,
                overflow_pending: false,
                rescan_pending: false,
                last_rescan_reason: None,
                last_error: None,
                last_audit_started_at_us: None,
                last_audit_completed_at_us: None,
                last_audit_duration_ms: None,
                emitted_batch_count: 0,
                emitted_event_count: 0,
                emitted_control_event_count: 0,
                emitted_data_event_count: 0,
                emitted_path_capture_target: None,
                emitted_path_event_count: 0,
                last_emitted_at_us: None,
                last_emitted_origins: Vec::new(),
                forwarded_batch_count: 0,
                forwarded_event_count: 0,
                forwarded_path_event_count: 0,
                last_forwarded_at_us: None,
                last_forwarded_origins: Vec::new(),
                current_revision: None,
                current_stream_generation: None,
                candidate_revision: None,
                candidate_stream_generation: None,
                candidate_status: None,
                draining_revision: None,
                draining_stream_generation: None,
                draining_status: None,
            },
            crate::source::SourceConcreteRootHealthSnapshot {
                root_key: "root-b-key".into(),
                logical_root_id: "root-b".into(),
                object_ref: "obj-b".into(),
                status: "ok".into(),
                coverage_mode: "audit_only".into(),
                watch_enabled: false,
                scan_enabled: true,
                is_group_primary: true,
                active: true,
                watch_lru_capacity: 0,
                audit_interval_ms: 10_000,
                overflow_count: 0,
                overflow_pending: false,
                rescan_pending: false,
                last_rescan_reason: None,
                last_error: None,
                last_audit_started_at_us: None,
                last_audit_completed_at_us: None,
                last_audit_duration_ms: None,
                emitted_batch_count: 0,
                emitted_event_count: 0,
                emitted_control_event_count: 0,
                emitted_data_event_count: 0,
                emitted_path_capture_target: None,
                emitted_path_event_count: 0,
                last_emitted_at_us: None,
                last_emitted_origins: Vec::new(),
                forwarded_batch_count: 0,
                forwarded_event_count: 0,
                forwarded_path_event_count: 0,
                last_forwarded_at_us: None,
                last_forwarded_origins: Vec::new(),
                current_revision: None,
                current_stream_generation: None,
                candidate_revision: None,
                candidate_stream_generation: None,
                candidate_status: None,
                draining_revision: None,
                draining_stream_generation: None,
                draining_status: None,
            },
        ],
        degraded_roots: Vec::new(),
    };
    let sink_status = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "root-b".into(),
            primary_object_ref: "obj-b".into(),
            total_nodes: 4,
            live_nodes: 4,
            tombstoned_count: 0,
            attested_count: 4,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 1,
            shadow_lag_us: 0,
            overflow_pending_audit: false,
            initial_audit_completed: true,
            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 1,
            estimated_heap_bytes: 1,
        }],
        ..SinkStatusSnapshot::default()
    };
    let filtered_source =
        filter_source_status_snapshot(source_status, &BTreeSet::from(["root-b".to_string()]));
    assert!(materialized_query_readiness_error(&filtered_source, &sink_status).is_none());
}
