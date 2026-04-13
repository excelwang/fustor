#[test]
fn merge_sink_status_snapshots_preserves_received_origin_counts_by_node() {
    let merged = merge_sink_status_snapshots(vec![
        SinkStatusSnapshot {
            scheduled_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            received_batches_by_node: BTreeMap::from([("node-a".to_string(), 11)]),
            received_events_by_node: BTreeMap::from([("node-a".to_string(), 111)]),
            received_control_events_by_node: BTreeMap::from([("node-a".to_string(), 2)]),
            received_data_events_by_node: BTreeMap::from([("node-a".to_string(), 109)]),
            last_received_at_us_by_node: BTreeMap::from([("node-a".to_string(), 123)]),
            last_received_origins_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["node-a::nfs1=2".to_string()],
            )]),
            received_origin_counts_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["node-a::nfs1=111".to_string()],
            )]),
            stream_applied_batches_by_node: BTreeMap::from([("node-a".to_string(), 5)]),
            stream_applied_events_by_node: BTreeMap::from([("node-a".to_string(), 50)]),
            stream_applied_control_events_by_node: BTreeMap::from([("node-a".to_string(), 4)]),
            stream_applied_data_events_by_node: BTreeMap::from([("node-a".to_string(), 46)]),
            stream_applied_origin_counts_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["node-a::nfs1=50".to_string()],
            )]),
            stream_last_applied_at_us_by_node: BTreeMap::from([("node-a".to_string(), 321)]),
            ..SinkStatusSnapshot::default()
        },
        SinkStatusSnapshot {
            scheduled_groups_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs2".to_string()],
            )]),
            received_batches_by_node: BTreeMap::from([("node-b".to_string(), 22)]),
            received_events_by_node: BTreeMap::from([("node-b".to_string(), 222)]),
            received_control_events_by_node: BTreeMap::from([("node-b".to_string(), 3)]),
            received_data_events_by_node: BTreeMap::from([("node-b".to_string(), 219)]),
            last_received_at_us_by_node: BTreeMap::from([("node-b".to_string(), 456)]),
            last_received_origins_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["node-b::nfs2=1".to_string()],
            )]),
            received_origin_counts_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["node-b::nfs2=222".to_string()],
            )]),
            stream_applied_batches_by_node: BTreeMap::from([("node-b".to_string(), 6)]),
            stream_applied_events_by_node: BTreeMap::from([("node-b".to_string(), 60)]),
            stream_applied_control_events_by_node: BTreeMap::from([("node-b".to_string(), 5)]),
            stream_applied_data_events_by_node: BTreeMap::from([("node-b".to_string(), 55)]),
            stream_applied_origin_counts_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["node-b::nfs2=60".to_string()],
            )]),
            stream_last_applied_at_us_by_node: BTreeMap::from([("node-b".to_string(), 654)]),
            ..SinkStatusSnapshot::default()
        },
    ]);

    assert_eq!(merged.received_batches_by_node.get("node-a"), Some(&11));
    assert_eq!(merged.received_batches_by_node.get("node-b"), Some(&22));
    assert_eq!(merged.received_events_by_node.get("node-a"), Some(&111));
    assert_eq!(merged.received_events_by_node.get("node-b"), Some(&222));
    assert_eq!(
        merged.stream_applied_batches_by_node.get("node-a"),
        Some(&5)
    );
    assert_eq!(
        merged.stream_applied_batches_by_node.get("node-b"),
        Some(&6)
    );
    assert_eq!(
        merged.stream_applied_events_by_node.get("node-a"),
        Some(&50)
    );
    assert_eq!(
        merged.stream_applied_events_by_node.get("node-b"),
        Some(&60)
    );
    assert_eq!(
        merged.last_received_origins_by_node.get("node-a"),
        Some(&vec!["node-a::nfs1=2".to_string()])
    );
    assert_eq!(
        merged.last_received_origins_by_node.get("node-b"),
        Some(&vec!["node-b::nfs2=1".to_string()])
    );
    assert_eq!(
        merged.received_origin_counts_by_node.get("node-a"),
        Some(&vec!["node-a::nfs1=111".to_string()])
    );
    assert_eq!(
        merged.received_origin_counts_by_node.get("node-b"),
        Some(&vec!["node-b::nfs2=222".to_string()])
    );
}

#[test]
fn merge_sink_status_snapshots_unions_scheduled_groups_for_same_node_across_partial_snapshots() {
    let merged = merge_sink_status_snapshots(vec![
        SinkStatusSnapshot {
            scheduled_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            )]),
            groups: vec![
                sink_group_status("nfs1", true),
                sink_group_status("nfs2", true),
            ],
            ..SinkStatusSnapshot::default()
        },
        SinkStatusSnapshot {
            scheduled_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            groups: vec![sink_group_status("nfs1", true)],
            ..SinkStatusSnapshot::default()
        },
        SinkStatusSnapshot {
            scheduled_groups_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs3".to_string()],
            )]),
            groups: vec![sink_group_status("nfs3", true)],
            ..SinkStatusSnapshot::default()
        },
    ]);

    assert_eq!(
        merged.scheduled_groups_by_node.get("node-a"),
        Some(&vec!["nfs1".to_string(), "nfs2".to_string()]),
        "same-node partial sink-status snapshots must union scheduled groups instead of letting a later partial view erase nfs2"
    );
    assert_eq!(
        merged.scheduled_groups_by_node.get("node-b"),
        Some(&vec!["nfs3".to_string()]),
    );
}
