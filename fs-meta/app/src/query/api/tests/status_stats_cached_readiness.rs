fn sink_group_status(
    group_id: &str,
    initial_audit_completed: bool,
) -> crate::sink::SinkGroupStatusSnapshot {
    crate::sink::SinkGroupStatusSnapshot {
        group_id: group_id.to_string(),
        primary_object_ref: format!("{group_id}-owner"),
        total_nodes: 1,
        live_nodes: 1,
        tombstoned_count: 0,
        attested_count: 0,
        suspect_count: 0,
        blind_spot_count: 0,
        shadow_time_us: 1,
        shadow_lag_us: 0,
        overflow_pending_materialization: false,
        readiness: if initial_audit_completed {
            crate::sink::GroupReadinessState::Ready
        } else {
            crate::sink::GroupReadinessState::PendingMaterialization
        },
        materialized_revision: 1,
        estimated_heap_bytes: 0,
    }
}

fn source_status_for_cache_key(group_id: &str) -> SourceStatusSnapshot {
    SourceStatusSnapshot {
        current_stream_generation: Some(7),
        logical_roots: vec![crate::source::SourceLogicalRootHealthSnapshot {
            root_id: group_id.to_string(),
            status: "ok".to_string(),
            matched_grants: 1,
            active_members: 1,
            coverage_mode: "realtime_hotset_plus_audit".to_string(),
        }],
        concrete_roots: vec![crate::source::SourceConcreteRootHealthSnapshot {
            root_key: format!("{group_id}@node-a"),
            logical_root_id: group_id.to_string(),
            object_ref: format!("node-a::{group_id}"),
            status: "ok".to_string(),
            coverage_mode: "realtime_hotset_plus_audit".to_string(),
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
            emitted_batch_count: 1,
            emitted_event_count: 1,
            emitted_control_event_count: 1,
            emitted_data_event_count: 1,
            emitted_path_capture_target: None,
            emitted_path_event_count: 1,
            last_emitted_at_us: Some(30),
            last_emitted_origins: vec!["node-a".to_string()],
            forwarded_batch_count: 1,
            forwarded_event_count: 1,
            forwarded_path_event_count: 1,
            last_forwarded_at_us: Some(40),
            last_forwarded_origins: vec!["node-a".to_string()],
            current_revision: Some(3),
            current_stream_generation: Some(7),
            candidate_revision: None,
            candidate_stream_generation: None,
            candidate_status: None,
            draining_revision: None,
            draining_stream_generation: None,
            draining_status: None,
        }],
        degraded_roots: Vec::new(),
    }
}

fn sink_status_for_cache_key(group_id: &str) -> SinkStatusSnapshot {
    let mut group = sink_group_status(group_id, true);
    group.shadow_time_us = 100;
    group.shadow_lag_us = 1;
    group.estimated_heap_bytes = 4096;
    SinkStatusSnapshot {
        live_nodes: 1,
        groups: vec![group],
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec![group_id.to_string()],
        )]),
        received_batches_by_node: BTreeMap::from([("node-a".to_string(), 1)]),
        received_events_by_node: BTreeMap::from([("node-a".to_string(), 2)]),
        last_received_at_us_by_node: BTreeMap::from([("node-a".to_string(), 300)]),
        stream_applied_events_by_node: BTreeMap::from([("node-a".to_string(), 2)]),
        stream_last_applied_at_us_by_node: BTreeMap::from([("node-a".to_string(), 400)]),
        ..SinkStatusSnapshot::default()
    }
}

#[test]
fn materialized_read_snapshot_cache_key_ignores_same_epoch_diagnostic_counters() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root = tmp.path().join("node-a-nfs1");
    fs::create_dir_all(&root).expect("create nfs1 root");
    let grants = vec![granted_mount_root("node-a::nfs1", &root)];
    let source = source_facade_with_group("nfs1", &grants);
    let sink = sink_facade_with_group(&grants);
    let state = test_api_state_for_source(source, sink);
    let params = normalize_api_params(ApiParams {
        path: Some("/".to_string()),
        path_b64: None,
        group: None,
        recursive: Some(true),
        max_depth: None,
        pit_id: None,
        group_order: None,
        group_page_size: None,
        group_after: None,
        entry_page_size: None,
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    })
    .expect("normalize params");

    let source_status = source_status_for_cache_key("nfs1");
    let sink_status = sink_status_for_cache_key("nfs1");
    let mut source_status_with_later_diagnostics = source_status.clone();
    let source_root = source_status_with_later_diagnostics
        .concrete_roots
        .get_mut(0)
        .expect("source root");
    source_root.emitted_event_count += 100;
    source_root.forwarded_event_count += 100;
    source_root.last_emitted_at_us = Some(1_000_000);
    source_root.last_forwarded_at_us = Some(1_000_100);
    let mut sink_status_with_later_diagnostics = sink_status.clone();
    sink_status_with_later_diagnostics
        .received_events_by_node
        .insert("node-a".to_string(), 200);
    sink_status_with_later_diagnostics
        .last_received_at_us_by_node
        .insert("node-a".to_string(), 1_000_200);
    sink_status_with_later_diagnostics
        .stream_applied_events_by_node
        .insert("node-a".to_string(), 200);
    sink_status_with_later_diagnostics
        .stream_last_applied_at_us_by_node
        .insert("node-a".to_string(), 1_000_300);
    let group = sink_status_with_later_diagnostics
        .groups
        .get_mut(0)
        .expect("sink group");
    group.shadow_time_us += 100;
    group.shadow_lag_us += 100;
    group.estimated_heap_bytes += 1024;

    let first_tree_key = materialized_tree_cache_key(
        &state,
        &params,
        ReadClass::TrustedMaterialized,
        Some(&source_status),
        Some(&sink_status),
    )
    .expect("first tree key");
    let later_tree_key = materialized_tree_cache_key(
        &state,
        &params,
        ReadClass::TrustedMaterialized,
        Some(&source_status_with_later_diagnostics),
        Some(&sink_status_with_later_diagnostics),
    )
    .expect("later tree key");
    assert_eq!(
        first_tree_key, later_tree_key,
        "same-epoch diagnostic counters must not invalidate steady trusted-materialized tree cache"
    );

    let first_stats_key = materialized_stats_cache_key(
        &state,
        &params,
        ReadClass::TrustedMaterialized,
        Some(&source_status),
        Some(&sink_status),
    )
    .expect("first stats key");
    let later_stats_key = materialized_stats_cache_key(
        &state,
        &params,
        ReadClass::TrustedMaterialized,
        Some(&source_status_with_later_diagnostics),
        Some(&sink_status_with_later_diagnostics),
    )
    .expect("later stats key");
    assert_eq!(
        first_stats_key, later_stats_key,
        "same-epoch diagnostic counters must not invalidate steady trusted-materialized stats cache"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_materialized_status_load_reuses_same_epoch_readiness_cache_without_route_fanin()
{
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a-nfs1");
    fs::create_dir_all(&node_a_root).expect("create node-a nfs1 dir");
    let grants = vec![granted_mount_root("node-a::nfs1", &node_a_root)];
    let source = source_facade_with_group("nfs1", &grants);
    let sink = sink_facade_with_group(&grants);
    let source_status_payload =
        source_observability_payload_for_runner_nodes("nfs1", &grants, &["node-a"]);
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string()],
        )]),
        groups: vec![sink_group_status("nfs1", true)],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink status");
    let boundary = Arc::new(SourceStatusMissingRouteStateThenReplyBoundary::new(
        source_status_payload,
        sink_status_payload,
    ));
    let source_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
        .expect("resolve source-status route");
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let state = ApiState {
        backend: QueryBackend::Route {
            sink: sink.clone(),
            boundary: boundary.clone(),
            origin_id: NodeId("node-d".to_string()),
            source: source.clone(),
        },
        policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
        pit_store: Arc::new(Mutex::new(QueryPitStore::default())),
        force_find_inflight: Arc::new(Mutex::new(BTreeSet::new())),
        force_find_runner_evidence: crate::api::state::ForceFindRunnerEvidence::default(),
        force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
        readiness_source: Some(source),
        readiness_sink: Some(sink),
        materialized_source_status_cache: Arc::new(Mutex::new(None)),
        materialized_sink_status_cache: Arc::new(Mutex::new(None)),
        materialized_stats_cache: Arc::new(Mutex::new(None)),
        materialized_tree_cache: Arc::new(Mutex::new(None)),
        tree_query_serial: Arc::new(tokio::sync::Mutex::new(())),
    };

    let first = load_materialized_status_snapshots(&state)
        .await
        .expect("first materialized status load should prime the readiness cache");
    assert_eq!(first.0.logical_roots.len(), 1);
    assert_eq!(first.1.groups.len(), 1);
    let source_sends_after_first = boundary.send_batch_count(&source_status_route.0);
    let sink_sends_after_first = boundary.send_batch_count(&sink_status_route.0);
    assert!(source_sends_after_first > 0);
    assert!(sink_sends_after_first > 0);

    let second = load_materialized_status_snapshots(&state)
        .await
        .expect("second materialized status load should reuse same-epoch readiness cache");
    assert_eq!(second.0.logical_roots.len(), 1);
    assert_eq!(second.1.groups.len(), 1);
    assert_eq!(
        boundary.send_batch_count(&source_status_route.0),
        source_sends_after_first,
        "same-epoch materialized reads should not re-run source status fan-in once readiness is cached"
    );
    assert_eq!(
        boundary.send_batch_count(&sink_status_route.0),
        sink_sends_after_first,
        "same-epoch materialized reads should not re-run sink status fan-in once readiness is cached"
    );
}

struct RequestScopedSinkStatusStalledBoundary {
    sink_reply_channel: String,
    send_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
}

impl RequestScopedSinkStatusStalledBoundary {
    fn new() -> Self {
        let sink_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
            .expect("resolve sink-status route");
        Self {
            sink_reply_channel: format!("{}:reply", sink_route.0),
            send_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
        }
    }

    fn send_batch_count(&self, channel: &str) -> usize {
        *self
            .send_batches_by_channel
            .lock()
            .expect("request-scoped stalled boundary send batches lock")
            .get(channel)
            .unwrap_or(&0)
    }
}

#[async_trait]
impl ChannelIoSubset for RequestScopedSinkStatusStalledBoundary {
    async fn channel_send(
        &self,
        _ctx: BoundaryContext,
        request: ChannelSendRequest,
    ) -> capanix_app_sdk::Result<()> {
        let mut send_batches = self
            .send_batches_by_channel
            .lock()
            .expect("request-scoped stalled boundary send batches lock");
        *send_batches.entry(request.channel_key.0).or_default() += 1;
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> capanix_app_sdk::Result<Vec<Event>> {
        if request.channel_key.0 == self.sink_reply_channel {
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        Err(CnxError::Timeout)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_scoped_sink_status_uses_short_ready_cache_grace_before_falling_back_to_same_epoch_readiness()
{
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a-nfs1");
    fs::create_dir_all(&node_a_root).expect("create node-a nfs1 dir");
    let grants = vec![granted_mount_root("node-a::nfs1", &node_a_root)];
    let source = source_facade_with_group("nfs1", &grants);
    let sink = sink_facade_with_group(&grants);
    let source_status_payload =
        source_observability_payload_for_runner_nodes("nfs1", &grants, &["node-a"]);
    let source_observability =
        rmp_serde::from_slice::<SourceObservabilitySnapshot>(&source_status_payload)
            .expect("decode source observability");
    let sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string()],
        )]),
        groups: vec![sink_group_status("nfs1", true)],
        ..SinkStatusSnapshot::default()
    };
    let sink_status_payload =
        rmp_serde::to_vec_named(&sink_status).expect("encode sink status");
    let _ = sink_status_payload;
    let boundary = Arc::new(RequestScopedSinkStatusStalledBoundary::new());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let state = ApiState {
        backend: QueryBackend::Route {
            sink: sink.clone(),
            boundary: boundary.clone(),
            origin_id: NodeId("node-d".to_string()),
            source: source.clone(),
        },
        policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
        pit_store: Arc::new(Mutex::new(QueryPitStore::default())),
        force_find_inflight: Arc::new(Mutex::new(BTreeSet::new())),
        force_find_runner_evidence: crate::api::state::ForceFindRunnerEvidence::default(),
        force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
        readiness_source: Some(source.clone()),
        readiness_sink: Some(sink),
        materialized_source_status_cache: Arc::new(Mutex::new(Some(
            CachedSourceStatusSnapshot {
                snapshot: source_observability.status,
                signature: materialized_readiness_cache_signature(&source)
                    .expect("readiness cache signature"),
            },
        ))),
        materialized_sink_status_cache: Arc::new(Mutex::new(Some(CachedSinkStatusSnapshot {
            snapshot: sink_status,
        }))),
        materialized_stats_cache: Arc::new(Mutex::new(None)),
        materialized_tree_cache: Arc::new(Mutex::new(None)),
        tree_query_serial: Arc::new(tokio::sync::Mutex::new(())),
    };

    let snapshot = tokio::time::timeout(
        Duration::from_millis(250),
        load_request_scoped_materialized_sink_status_snapshot(
            &state,
            TreePitSessionPlan::new(Duration::from_millis(500), 1)
                .request_scoped_sink_status_plan(),
        ),
    )
    .await
    .expect("request-scoped ready-cache fallback must not wait for the full route timeout")
    .expect("request-scoped sink status should reuse cached readiness");

    assert_eq!(snapshot.groups.len(), 1);
    assert_eq!(
        boundary.send_batch_count(&sink_status_route.0),
        1,
        "same-epoch trusted-materialized reads may probe request-scoped sink-status once, but must fall back through the short ready-cache grace when the route stalls"
    );
}

#[test]
fn cached_sink_status_preserves_complete_snapshot_for_same_roots() {
    let groups = BTreeSet::from(["nfs1".to_string(), "nfs2".to_string(), "nfs3".to_string()]);
    let cached = CachedSinkStatusSnapshot {
        snapshot: SinkStatusSnapshot {
            groups: vec![
                sink_group_status("nfs1", true),
                sink_group_status("nfs2", true),
                sink_group_status("nfs3", true),
            ],
            ..SinkStatusSnapshot::default()
        },
    };
    let fresh = SinkStatusSnapshot {
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
        ],
        ..SinkStatusSnapshot::default()
    };

    let merged = merge_with_cached_sink_status_snapshot(Some(&cached), &groups, fresh);
    let merged_groups = merged
        .groups
        .iter()
        .map(|group| group.group_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(merged_groups, vec!["nfs1", "nfs2", "nfs3"]);
    assert!(
        merged
            .groups
            .iter()
            .all(|group| group.is_ready())
    );
}

#[test]
fn materialized_scheduled_group_ids_preserves_groups_present_in_snapshot_when_scheduled_map_is_partial()
 {
    let snapshot = SinkStatusSnapshot {
        groups: vec![
            sink_group_status("nfs1", false),
            sink_group_status("nfs2", false),
            sink_group_status("nfs3", true),
        ],
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs3".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };

    let groups = materialized_scheduled_group_ids(&snapshot)
        .into_iter()
        .collect::<Vec<_>>();

    assert_eq!(groups, vec!["nfs1", "nfs2", "nfs3"]);
}

#[test]
fn cached_sink_status_drops_stale_groups_after_root_set_change() {
    let cached = CachedSinkStatusSnapshot {
        snapshot: SinkStatusSnapshot {
            groups: vec![
                sink_group_status("nfs1", true),
                sink_group_status("nfs2", true),
            ],
            ..SinkStatusSnapshot::default()
        },
    };
    let fresh_groups = BTreeSet::from(["nfs1".to_string()]);
    let fresh = SinkStatusSnapshot {
        groups: vec![sink_group_status("nfs1", true)],
        ..SinkStatusSnapshot::default()
    };

    let merged = merge_with_cached_sink_status_snapshot(Some(&cached), &fresh_groups, fresh);
    let merged_groups = merged
        .groups
        .iter()
        .map(|group| group.group_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(merged_groups, vec!["nfs1"]);
}

#[test]
fn local_sink_snapshot_explicit_empty_clears_preserved_cached_ready_groups_after_routed_transition()
{
    let source_status = SourceStatusSnapshot {
        current_stream_generation: Some(1),
        logical_roots: vec![
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".into(),
                status: "ok".into(),
                matched_grants: 1,
                active_members: 1,
                coverage_mode: "realtime_hotset_plus_audit".into(),
            },
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "nfs2".into(),
                status: "ok".into(),
                matched_grants: 1,
                active_members: 1,
                coverage_mode: "realtime_hotset_plus_audit".into(),
            },
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "nfs3".into(),
                status: "ok".into(),
                matched_grants: 1,
                active_members: 1,
                coverage_mode: "realtime_hotset_plus_audit".into(),
            },
        ],
        concrete_roots: Vec::new(),
        degraded_roots: Vec::new(),
    };
    let cached_snapshot = SinkStatusSnapshot {
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string(), "nfs3".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };
    let cached = CachedSinkStatusSnapshot {
        snapshot: cached_snapshot.clone(),
    };
    let allowed_groups =
        BTreeSet::from(["nfs1".to_string(), "nfs2".to_string(), "nfs3".to_string()]);
    let routed_sink_status = SinkStatusSnapshot {
        groups: vec![
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs1".to_string(),
                primary_object_ref: "node-a::nfs1".to_string(),
                total_nodes: 0,
                live_nodes: 0,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::PendingMaterialization,
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            },
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs2".to_string(),
                primary_object_ref: "node-a::nfs2".to_string(),
                total_nodes: 0,
                live_nodes: 0,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::PendingMaterialization,
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            },
            sink_group_status("nfs3", true),
        ],
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string(), "nfs3".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };
    let merged =
        merge_with_cached_sink_status_snapshot(Some(&cached), &allowed_groups, routed_sink_status);
    let preserved = preserve_cached_ready_groups_across_explicit_empty_root_transition(
        merged,
        Some(&cached_snapshot),
        &source_status,
        &BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]),
    );
    let local_sink_status = SinkStatusSnapshot {
        groups: vec![
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs1".to_string(),
                primary_object_ref: "node-a::nfs1".to_string(),
                total_nodes: 0,
                live_nodes: 0,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::PendingMaterialization,
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            },
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs2".to_string(),
                primary_object_ref: "node-a::nfs2".to_string(),
                total_nodes: 0,
                live_nodes: 0,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::PendingMaterialization,
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            },
            sink_group_status("nfs3", true),
        ],
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string(), "nfs3".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };
    let route_explicit_empty_groups = BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let explicit_empty_groups_to_clear = local_sink_snapshot_route_preserved_explicit_empty_groups(
        &preserved,
        &local_sink_status,
        &route_explicit_empty_groups,
    );

    assert!(
        !sink_status_snapshot_has_better_groups(&preserved, &local_sink_status),
        "generic local snapshot comparison must not treat same-group explicit-empty as globally better when the only correction needed is to clear route-preserved stale ready groups"
    );
    assert_eq!(
        explicit_empty_groups_to_clear, route_explicit_empty_groups,
        "only the route-explicit-empty groups should be cleared from the preserved snapshot before merging the local sink snapshot"
    );

    let merged = merge_with_local_sink_status_snapshot(
        preserved,
        local_sink_status,
        &explicit_empty_groups_to_clear,
    );
    let merged_groups = merged
        .groups
        .iter()
        .map(|group| {
            (
                group.group_id.as_str(),
                group.is_ready(),
                group.total_nodes,
                group.live_nodes,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        merged_groups,
        vec![
            ("nfs1", false, 0, 0),
            ("nfs2", false, 0, 0),
            ("nfs3", true, 1, 1),
        ],
        "local same-group explicit-empty sink status must clear route-preserved cached ready groups instead of leaving stale trusted-materialized nfs1/nfs2 in the merged snapshot: {merged_groups:?}"
    );
}

#[test]
fn materialized_query_readiness_preserves_retained_group_audit_across_root_set_contraction() {
    let source_status = SourceStatusSnapshot {
        current_stream_generation: Some(1),
        logical_roots: vec![crate::source::SourceLogicalRootHealthSnapshot {
            root_id: "nfs2".into(),
            status: "ok".into(),
            matched_grants: 3,
            active_members: 3,
            coverage_mode: "realtime_hotset_plus_audit".into(),
        }],
        concrete_roots: vec![crate::source::SourceConcreteRootHealthSnapshot {
            root_key: "nfs2-key".into(),
            logical_root_id: "nfs2".into(),
            object_ref: "node-a::nfs2".into(),
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
            last_rescan_reason: Some("manual".into()),
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
    let cached = CachedSinkStatusSnapshot {
        snapshot: SinkStatusSnapshot {
            groups: vec![
                sink_group_status("nfs1", true),
                sink_group_status("nfs2", true),
                sink_group_status("nfs3", true),
                sink_group_status("nfs4", false),
            ],
            ..SinkStatusSnapshot::default()
        },
    };
    let allowed_groups = BTreeSet::from(["nfs2".to_string(), "nfs4".to_string()]);
    let fresh = SinkStatusSnapshot {
        groups: vec![
            sink_group_status("nfs2", false),
            sink_group_status("nfs4", false),
        ],
        ..SinkStatusSnapshot::default()
    };

    let merged = merge_with_cached_sink_status_snapshot(Some(&cached), &allowed_groups, fresh);
    let merged_groups = merged
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group.is_ready()))
        .collect::<Vec<_>>();
    assert_eq!(merged_groups, vec![("nfs2", true), ("nfs4", false)]);
    assert!(
        materialized_query_readiness_error(&source_status, &merged).is_none(),
        "retained nfs2 should stay trusted-materialized ready across retire root contraction"
    );
}

#[test]
fn cached_sink_status_preserves_missing_nfs2_readiness_when_later_fresh_snapshot_only_reports_other_groups()
 {
    let source_status = SourceStatusSnapshot {
        current_stream_generation: None,
        logical_roots: vec![
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".into(),
                status: "ok".into(),
                matched_grants: 1,
                active_members: 1,
                coverage_mode: "realtime_hotset_plus_audit".into(),
            },
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "nfs2".into(),
                status: "ok".into(),
                matched_grants: 1,
                active_members: 1,
                coverage_mode: "realtime_hotset_plus_audit".into(),
            },
        ],
        concrete_roots: Vec::new(),
        degraded_roots: Vec::new(),
    };
    let cached = CachedSinkStatusSnapshot {
        snapshot: SinkStatusSnapshot {
            groups: vec![
                sink_group_status("nfs1", true),
                sink_group_status("nfs2", true),
            ],
            ..SinkStatusSnapshot::default()
        },
    };
    let allowed_groups = BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let fresh = SinkStatusSnapshot {
        groups: vec![sink_group_status("nfs1", true)],
        ..SinkStatusSnapshot::default()
    };

    let merged = merge_with_cached_sink_status_snapshot(Some(&cached), &allowed_groups, fresh);
    let merged_groups = merged
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group.is_ready()))
        .collect::<Vec<_>>();
    assert_eq!(merged_groups, vec![("nfs1", true), ("nfs2", true)]);
    assert!(
        materialized_query_readiness_error(&source_status, &merged).is_none(),
        "later fresh snapshots that only mention other groups must not make cached nfs2 readiness disappear again"
    );
}
