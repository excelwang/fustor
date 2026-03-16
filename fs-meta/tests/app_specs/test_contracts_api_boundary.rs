use std::collections::BTreeSet;
use std::thread;
use std::time::{Duration, Instant};

use crate::app_support::fixture_support::{FsMetaApiFixture, HttpResponse, http_json};
use serde_json::json;

fn expect_error_field(resp: &HttpResponse) {
    let json = resp
        .json
        .as_ref()
        .expect("error response should be JSON payload");
    let err = json
        .get("error")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert!(
        !err.is_empty(),
        "error response should include non-empty error field, body={}",
        resp.body
    );
}

fn response_group_keys(resp: &HttpResponse) -> BTreeSet<String> {
    resp.json
        .as_ref()
        .and_then(|v| v.get("groups"))
        .and_then(|v| v.as_array())
        .map(|groups| {
            groups
                .iter()
                .filter_map(|group| group.get("group").and_then(|v| v.as_str()))
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn create_query_api_key(base: &str, management_token: &str, label: &str) -> (String, String) {
    let response = http_json(
        base,
        "POST",
        "/api/fs-meta/v1/query-api-keys",
        Some(management_token),
        Some(&json!({ "label": label })),
    )
    .expect("create query api key");
    assert_eq!(response.status, 200, "body={}", response.body);
    let api_key = response
        .json
        .as_ref()
        .and_then(|value| value.get("api_key"))
        .and_then(|value| value.as_str())
        .expect("api_key")
        .to_string();
    let key_id = response
        .json
        .as_ref()
        .and_then(|value| value.get("key"))
        .and_then(|value| value.get("key_id"))
        .and_then(|value| value.as_str())
        .expect("key_id")
        .to_string();
    (api_key, key_id)
}

fn wait_for_group_keys(
    base: &str,
    token: &str,
    path: &str,
    expected: &BTreeSet<String>,
) -> HttpResponse {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut last = None;
    while Instant::now() < deadline {
        let resp = http_json(base, "GET", path, Some(token), None).expect("query endpoint");
        let keys = response_group_keys(&resp);
        if &keys == expected {
            return resp;
        }
        last = Some(resp);
        thread::sleep(Duration::from_millis(150));
    }
    let last = last.expect("at least one response before timeout");
    panic!(
        "group keys for {path} did not converge; expected={expected:?} got={:?} body={}",
        response_group_keys(&last),
        last.body
    );
}

#[test]
fn blackbox_management_api_contract() {
    let fixture = match FsMetaApiFixture::start() {
        Ok(fixture) => fixture,
        Err(err) if err.contains("Operation not permitted") => return,
        Err(err) => panic!("start fs-meta api fixture: {err}"),
    };
    let base = fixture.api_base_url();

    let login_admin = http_json(
        base,
        "POST",
        "/api/fs-meta/v1/session/login",
        None,
        Some(&json!({"username":"operator","password":"operator123"})),
    )
    .expect("admin login request");
    assert_eq!(login_admin.status, 200, "body={}", login_admin.body);
    let admin_token = login_admin
        .json
        .as_ref()
        .and_then(|v| v.get("token"))
        .and_then(|v| v.as_str())
        .expect("admin token")
        .to_string();

    let login_reader = http_json(
        base,
        "POST",
        "/api/fs-meta/v1/session/login",
        None,
        Some(&json!({"username":"reader","password":"reader123"})),
    )
    .expect("reader login request");
    assert_eq!(login_reader.status, 200);
    let reader_token = login_reader
        .json
        .as_ref()
        .and_then(|v| v.get("token"))
        .and_then(|v| v.as_str())
        .expect("reader token")
        .to_string();

    let wrong_password = http_json(
        base,
        "POST",
        "/api/fs-meta/v1/session/login",
        None,
        Some(&json!({"username":"operator","password":"wrong"})),
    )
    .expect("wrong password login request");
    assert_eq!(wrong_password.status, 401);
    expect_error_field(&wrong_password);

    let empty_username = http_json(
        base,
        "POST",
        "/api/fs-meta/v1/session/login",
        None,
        Some(&json!({"username":"","password":"x"})),
    )
    .expect("empty username login request");
    assert_eq!(empty_username.status, 400);
    expect_error_field(&empty_username);

    let reader_status = http_json(
        base,
        "GET",
        "/api/fs-meta/v1/status",
        Some(&reader_token),
        None,
    )
    .expect("reader status");
    assert_eq!(reader_status.status, 403, "body={}", reader_status.body);

    let status = http_json(
        base,
        "GET",
        "/api/fs-meta/v1/status",
        Some(&admin_token),
        None,
    )
    .expect("status");
    assert_eq!(status.status, 200, "body={}", status.body);
    assert_eq!(
        status
            .json
            .as_ref()
            .and_then(|v| v.get("source"))
            .and_then(|v| v.get("roots_count"))
            .and_then(|v| v.as_u64()),
        Some(2)
    );
    assert_eq!(
        status
            .json
            .as_ref()
            .and_then(|v| v.get("source"))
            .and_then(|v| v.get("logical_roots"))
            .and_then(|v| v.as_array())
            .map(|rows| rows.len()),
        Some(2)
    );
    assert!(
        status
            .json
            .as_ref()
            .and_then(|v| v.get("source"))
            .and_then(|v| v.get("logical_roots"))
            .and_then(|v| v.as_array())
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("coverage_mode"))
            .and_then(|v| v.as_str())
            .is_some(),
        "status.source.logical_roots[0].coverage_mode missing: {}",
        status.body
    );
    assert!(
        status
            .json
            .as_ref()
            .and_then(|v| v.get("source"))
            .and_then(|v| v.get("concrete_roots"))
            .and_then(|v| v.as_array())
            .is_some_and(|rows| !rows.is_empty()),
        "status.source.concrete_roots missing or empty: {}",
        status.body
    );
    assert!(
        status
            .json
            .as_ref()
            .and_then(|v| v.get("source"))
            .and_then(|v| v.get("concrete_roots"))
            .and_then(|v| v.as_array())
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("watch_lru_capacity"))
            .and_then(|v| v.as_u64())
            .is_some(),
        "status.source.concrete_roots[0].watch_lru_capacity missing: {}",
        status.body
    );
    assert!(
        status
            .json
            .as_ref()
            .and_then(|v| v.get("source"))
            .and_then(|v| v.get("concrete_roots"))
            .and_then(|v| v.as_array())
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("audit_interval_ms"))
            .and_then(|v| v.as_u64())
            .is_some(),
        "status.source.concrete_roots[0].audit_interval_ms missing: {}",
        status.body
    );
    assert!(
        status
            .json
            .as_ref()
            .and_then(|v| v.get("sink"))
            .and_then(|v| v.get("estimated_heap_bytes"))
            .and_then(|v| v.as_u64())
            .is_some(),
        "status.sink.estimated_heap_bytes missing: {}",
        status.body
    );
    assert!(
        status
            .json
            .as_ref()
            .and_then(|v| v.get("sink"))
            .and_then(|v| v.get("groups"))
            .and_then(|v| v.as_array())
            .is_some_and(|rows| !rows.is_empty()),
        "status.sink.groups missing or empty: {}",
        status.body
    );

    let grants = http_json(
        base,
        "GET",
        "/api/fs-meta/v1/runtime/grants",
        Some(&admin_token),
        None,
    )
    .expect("runtime grants");
    assert_eq!(grants.status, 200);
    assert_eq!(
        grants
            .json
            .as_ref()
            .and_then(|v| v.get("grants"))
            .and_then(|v| v.as_array())
            .map(|rows| rows.len()),
        Some(4)
    );

    let roots_before = http_json(
        base,
        "GET",
        "/api/fs-meta/v1/monitoring/roots",
        Some(&admin_token),
        None,
    )
    .expect("roots get");
    assert_eq!(roots_before.status, 200);
    assert_eq!(
        roots_before
            .json
            .as_ref()
            .and_then(|v| v.get("roots"))
            .and_then(|v| v.as_array())
            .map(|rows| rows.len()),
        Some(2)
    );

    let preview = http_json(
        base,
        "POST",
        "/api/fs-meta/v1/monitoring/roots/preview",
        Some(&admin_token),
        Some(&json!({
            "roots": [
                {"id":"root-a","selector":{"mount_point":fixture.root_a()},"subpath_scope":"/","watch":true,"scan":true},
                {"id":"missing","selector":{"mount_point":"/no/such/path"},"subpath_scope":"/","watch":true,"scan":true}
            ]
        })),
    )
    .expect("roots preview");
    assert_eq!(preview.status, 200, "body={}", preview.body);
    assert!(
        preview
            .json
            .as_ref()
            .and_then(|v| v.get("preview"))
            .and_then(|v| v.as_array())
            .is_some_and(|rows| rows.len() == 2)
    );
    assert_eq!(
        preview
            .json
            .as_ref()
            .and_then(|v| v.get("unmatched_roots"))
            .and_then(|v| v.as_array())
            .and_then(|rows| rows.first())
            .and_then(|v| v.as_str()),
        Some("missing")
    );

    let reader_preview = http_json(
        base,
        "POST",
        "/api/fs-meta/v1/monitoring/roots/preview",
        Some(&reader_token),
        Some(&json!({"roots": []})),
    )
    .expect("reader roots preview");
    assert_eq!(reader_preview.status, 403);

    let unmatched_write = http_json(
        base,
        "PUT",
        "/api/fs-meta/v1/monitoring/roots",
        Some(&admin_token),
        Some(&json!({
            "roots": [
                {"id":"root-a","selector":{"mount_point":fixture.root_a()},"subpath_scope":"/","watch":true,"scan":true},
                {"id":"missing","selector":{"mount_point":"/no/such/path"},"subpath_scope":"/","watch":true,"scan":true}
            ]
        })),
    )
    .expect("unmatched roots put");
    assert_eq!(unmatched_write.status, 400, "body={}", unmatched_write.body);
    expect_error_field(&unmatched_write);
    assert!(
        unmatched_write.body.contains("missing"),
        "unmatched apply rejection should keep offending root id explicit: {}",
        unmatched_write.body
    );

    let roots_after_rejected_unmatched = http_json(
        base,
        "GET",
        "/api/fs-meta/v1/monitoring/roots",
        Some(&admin_token),
        None,
    )
    .expect("roots after rejected unmatched apply");
    assert_eq!(
        roots_after_rejected_unmatched
            .json
            .as_ref()
            .and_then(|v| v.get("roots"))
            .and_then(|v| v.as_array())
            .map(|rows| rows.len()),
        Some(2)
    );

    let legacy_path = http_json(
        base,
        "PUT",
        "/api/fs-meta/v1/monitoring/roots",
        Some(&admin_token),
        Some(&json!({
            "roots": [{"id":"root-a","path":fixture.root_a(),"watch":true,"scan":true}]
        })),
    )
    .expect("legacy path put");
    assert_eq!(legacy_path.status, 400);
    expect_error_field(&legacy_path);

    let reader_write = http_json(
        base,
        "PUT",
        "/api/fs-meta/v1/monitoring/roots",
        Some(&reader_token),
        Some(&json!({
            "roots": [{"id":"root-a","selector":{"mount_point":fixture.root_a()},"subpath_scope":"/","watch":true,"scan":true}]
        })),
    )
    .expect("reader roots put");
    assert_eq!(reader_write.status, 403);
    expect_error_field(&reader_write);

    let reader_grants = http_json(
        base,
        "GET",
        "/api/fs-meta/v1/runtime/grants",
        Some(&reader_token),
        None,
    )
    .expect("reader grants");
    assert_eq!(reader_grants.status, 403);

    let empty_update = http_json(
        base,
        "PUT",
        "/api/fs-meta/v1/monitoring/roots",
        Some(&admin_token),
        Some(&json!({"roots": []})),
    )
    .expect("empty roots update");
    assert_eq!(empty_update.status, 200, "body={}", empty_update.body);
    assert_eq!(
        empty_update
            .json
            .as_ref()
            .and_then(|v| v.get("roots_count"))
            .and_then(|v| v.as_u64()),
        Some(0)
    );

    let roots_after_empty = http_json(
        base,
        "GET",
        "/api/fs-meta/v1/monitoring/roots",
        Some(&admin_token),
        None,
    )
    .expect("roots after empty");
    assert_eq!(
        roots_after_empty
            .json
            .as_ref()
            .and_then(|v| v.get("roots"))
            .and_then(|v| v.as_array())
            .map(|rows| rows.len()),
        Some(0)
    );

    let restore = http_json(
        base,
        "PUT",
        "/api/fs-meta/v1/monitoring/roots",
        Some(&admin_token),
        Some(&json!({
            "roots": [
                {"id":"root-a-updated","selector":{"mount_point":fixture.root_a()},"subpath_scope":"/","watch":true,"scan":true},
                {"id":"root-b-updated","selector":{"mount_point":fixture.root_b()},"subpath_scope":"/","watch":true,"scan":true}
            ]
        })),
    )
    .expect("restore roots update");
    assert_eq!(restore.status, 200);
    assert_eq!(
        restore
            .json
            .as_ref()
            .and_then(|v| v.get("roots_count"))
            .and_then(|v| v.as_u64()),
        Some(2)
    );

    let rescan_reader = http_json(
        base,
        "POST",
        "/api/fs-meta/v1/index/rescan",
        Some(&reader_token),
        None,
    )
    .expect("reader rescan");
    assert_eq!(rescan_reader.status, 403);

    let rescan_admin = http_json(
        base,
        "POST",
        "/api/fs-meta/v1/index/rescan",
        Some(&admin_token),
        None,
    )
    .expect("admin rescan");
    assert_eq!(rescan_admin.status, 200);
    assert_eq!(
        rescan_admin
            .json
            .as_ref()
            .and_then(|v| v.get("accepted"))
            .and_then(|v| v.as_bool()),
        Some(true)
    );

    let create_key_reader = http_json(
        base,
        "POST",
        "/api/fs-meta/v1/query-api-keys",
        Some(&reader_token),
        Some(&json!({ "label": "reader-denied" })),
    )
    .expect("reader create query key");
    assert_eq!(create_key_reader.status, 403);

    let list_keys_before = http_json(
        base,
        "GET",
        "/api/fs-meta/v1/query-api-keys",
        Some(&admin_token),
        None,
    )
    .expect("list query keys before");
    assert_eq!(
        list_keys_before.status, 200,
        "body={}",
        list_keys_before.body
    );
    assert_eq!(
        list_keys_before
            .json
            .as_ref()
            .and_then(|value| value.get("keys"))
            .and_then(|value| value.as_array())
            .map(|rows| rows.len()),
        Some(0)
    );

    let (query_api_key, key_id) = create_query_api_key(base, &admin_token, "ui-monitoring");

    let tree_with_management_session = http_json(
        base,
        "GET",
        "/api/fs-meta/v1/tree?path=/",
        Some(&admin_token),
        None,
    )
    .expect("tree with management session");
    assert_eq!(tree_with_management_session.status, 401);

    let tree_with_query_key = http_json(
        base,
        "GET",
        "/api/fs-meta/v1/tree?path=/",
        Some(&query_api_key),
        None,
    )
    .expect("tree with query key");
    assert_eq!(
        tree_with_query_key.status, 200,
        "body={}",
        tree_with_query_key.body
    );
    assert!(
        tree_with_query_key
            .json
            .as_ref()
            .and_then(|value| value.get("groups"))
            .and_then(|value| value.as_array())
            .is_some_and(|rows| !rows.is_empty()),
        "tree groups missing: {}",
        tree_with_query_key.body
    );

    let stats_with_query_key = http_json(
        base,
        "GET",
        "/api/fs-meta/v1/stats?path=/",
        Some(&query_api_key),
        None,
    )
    .expect("stats with query key");
    assert_eq!(
        stats_with_query_key.status, 200,
        "body={}",
        stats_with_query_key.body
    );
    assert!(
        stats_with_query_key
            .json
            .as_ref()
            .and_then(|value| value.get("groups"))
            .and_then(|value| value.as_object())
            .is_some_and(|rows| !rows.is_empty()),
        "stats groups missing: {}",
        stats_with_query_key.body
    );

    let status_with_query_key = http_json(
        base,
        "GET",
        "/api/fs-meta/v1/status",
        Some(&query_api_key),
        None,
    )
    .expect("status with query key");
    assert_eq!(status_with_query_key.status, 401);

    let revoke_query_key = http_json(
        base,
        "DELETE",
        &format!("/api/fs-meta/v1/query-api-keys/{key_id}"),
        Some(&admin_token),
        None,
    )
    .expect("revoke query key");
    assert_eq!(
        revoke_query_key.status, 200,
        "body={}",
        revoke_query_key.body
    );
    assert_eq!(
        revoke_query_key
            .json
            .as_ref()
            .and_then(|value| value.get("revoked"))
            .and_then(|value| value.as_bool()),
        Some(true)
    );

    let revoked_query_key = http_json(
        base,
        "GET",
        "/api/fs-meta/v1/tree?path=/",
        Some(&query_api_key),
        None,
    )
    .expect("revoked query key");
    assert_eq!(revoked_query_key.status, 401);

    for old_path in [
        "/api/fs-meta/v1/auth/login",
        "/api/fs-meta/v1/health",
        "/api/fs-meta/v1/exports",
        "/api/fs-meta/v1/fanout",
        "/api/fs-meta/v1/config/roots",
        "/api/fs-meta/v1/ops/rescan",
        "/api/fs-meta/v1/declaration/render",
    ] {
        let resp = http_json(base, "GET", old_path, Some(&admin_token), None)
            .unwrap_or_else(|err| panic!("old path probe failed for {old_path}: {err}"));
        assert!(
            resp.status == 404 || resp.status == 405,
            "old path {old_path} should not be served, got {} body={} ",
            resp.status,
            resp.body
        );
    }
}

#[test]
fn blackbox_group_reconfiguration_updates_query_and_force_find_groups() {
    let fixture = match FsMetaApiFixture::start() {
        Ok(fixture) => fixture,
        Err(err) if err.contains("Operation not permitted") => return,
        Err(err) => panic!("start fs-meta api fixture: {err}"),
    };
    let base = fixture.api_base_url();

    let login_admin = http_json(
        base,
        "POST",
        "/api/fs-meta/v1/session/login",
        None,
        Some(&json!({"username":"operator","password":"operator123"})),
    )
    .expect("admin login request");
    assert_eq!(login_admin.status, 200);
    let admin_token = login_admin
        .json
        .as_ref()
        .and_then(|v| v.get("token"))
        .and_then(|v| v.as_str())
        .expect("admin login token")
        .to_string();

    let (query_api_key, _) = create_query_api_key(base, &admin_token, "group-reconfig");

    let initial_groups = BTreeSet::from(["root-a".to_string(), "root-b".to_string()]);
    let tree_query = "/api/fs-meta/v1/tree?path=/";
    let force_find_query = "/api/fs-meta/v1/on-demand-force-find?path=/";
    let tree_resp = wait_for_group_keys(base, &query_api_key, tree_query, &initial_groups);
    assert_eq!(response_group_keys(&tree_resp), initial_groups);
    let force_resp = wait_for_group_keys(base, &query_api_key, force_find_query, &initial_groups);
    assert_eq!(response_group_keys(&force_resp), initial_groups);

    let split_groups = BTreeSet::from([
        "root-a-host-11".to_string(),
        "root-a-host-12".to_string(),
        "root-b".to_string(),
    ]);
    let split_payload = json!({
        "roots": [
            {"id":"root-a-host-11","selector":{"mount_point":fixture.root_a(),"host_ip":"10.0.0.11"},"subpath_scope":"/","watch":true,"scan":true},
            {"id":"root-a-host-12","selector":{"mount_point":fixture.root_a(),"host_ip":"10.0.0.12"},"subpath_scope":"/","watch":true,"scan":true},
            {"id":"root-b","selector":{"mount_point":fixture.root_b()},"subpath_scope":"/","watch":true,"scan":true}
        ]
    });
    let split_update = http_json(
        base,
        "PUT",
        "/api/fs-meta/v1/monitoring/roots",
        Some(&admin_token),
        Some(&split_payload),
    )
    .expect("split roots update");
    assert_eq!(split_update.status, 200, "body={}", split_update.body);

    let tree_resp = wait_for_group_keys(base, &query_api_key, tree_query, &split_groups);
    assert_eq!(response_group_keys(&tree_resp), split_groups);
    let force_resp = wait_for_group_keys(base, &query_api_key, force_find_query, &split_groups);
    assert_eq!(response_group_keys(&force_resp), split_groups);

    let removed_groups =
        BTreeSet::from(["root-a-host-11".to_string(), "root-a-host-12".to_string()]);
    let remove_payload = json!({
        "roots": [
            {"id":"root-a-host-11","selector":{"mount_point":fixture.root_a(),"host_ip":"10.0.0.11"},"subpath_scope":"/","watch":true,"scan":true},
            {"id":"root-a-host-12","selector":{"mount_point":fixture.root_a(),"host_ip":"10.0.0.12"},"subpath_scope":"/","watch":true,"scan":true}
        ]
    });
    let remove_update = http_json(
        base,
        "PUT",
        "/api/fs-meta/v1/monitoring/roots",
        Some(&admin_token),
        Some(&remove_payload),
    )
    .expect("remove roots update");
    assert_eq!(remove_update.status, 200, "body={}", remove_update.body);

    let tree_resp = wait_for_group_keys(base, &query_api_key, tree_query, &removed_groups);
    assert_eq!(response_group_keys(&tree_resp), removed_groups);
    let force_resp = wait_for_group_keys(base, &query_api_key, force_find_query, &removed_groups);
    assert_eq!(response_group_keys(&force_resp), removed_groups);
}
