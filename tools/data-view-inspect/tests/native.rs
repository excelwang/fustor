// ASSERTION INTENT (Black-box tests — public traits and APIs only). Do not introduce white-box testing logic or internal workarounds.
use serde_json::{Value, json};
use std::{
    fs,
    io::Write,
    path::Path,
    process::{Command, Stdio},
};

fn call(config: &Path, mode: &str, input: Value) -> (bool, Value) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_fustor-data-view-inspect"))
        .args([mode, "--config", config.to_str().unwrap()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.to_string().as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    (
        output.status.success(),
        serde_json::from_slice(if output.status.success() {
            &output.stdout
        } else {
            &output.stderr
        })
        .unwrap(),
    )
}

// @verify_spec("CONTRACTS.DATA_VIEW.DECLARED_EXTERNAL_RESULT_ADMISSION", mode="system")
#[test]
fn native_cli_admission_and_scoped_visibility_survive_restart_without_graph_coupling() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("native.json");
    let mut document = json!({"data_view_id":"test-view","state_path":dir.path().join("state.json"),"allowed_state_root":dir.path(),
      "declared_policies":[{"profile_id":"external-v1","producer_id":"test-external","result_authority_ref":"declared:test","schema":{"schema_ref":"manifest:test","schema_version":"1"},
        "allowed_external_ref_prefix":"file:///external/","max_manifest_bytes":65536,"max_external_size_bytes":1000000,"policy_context_digest":"test-policy","grant_epoch":1,"required_policy_labels":["candidate"],"max_ttl_ms":60000}],
      "scoped_request":{"revision_selector":{"kind":"latest"},"now_ms":0,"access":{"subject_scope_id":"test:operator","tenant_id":"test","principal_id":"operator","purpose":"inspection",
        "permitted_source_authority_refs":["declared:test"],"visible_policy_labels":["candidate"],"current_grant_epoch":1,"allow_stale_but_plannable":false,"coverage_evidence":"complete-for-scope",
        "discovery_budget":{"time_budget_ms":5000,"remaining_time_ms":5000,"remaining_calls":1,"remaining_rows":0,"remaining_bytes":0,"remaining_fanout":1,"remaining_cost_units":1},
        "disclosure_budget":{"max_results":10,"max_payload_bytes":65536,"max_relationships":1,"include_payloads":false,"include_evidence_refs":true}},
        "discovery_request":{"discovery_intent":{"task":"read candidate results","priorities":[]},"policy_context":{"user_id":"operator","tenant_id":"test","task_purpose":"inspection","allowed_source_instances":[],"grant_epoch":1,"request_context":{}},
          "discovery_scope":{"source_instances":[],"max_fragments":1},"time_budget_ms":5000,"disclosure_budget":{"max_fragments":1,"max_names":1,"max_relation_hints":1},"candidate_fragments":[],"scoped_data_view":null,"source_capabilities":[]}}});
    fs::write(&config, document.to_string()).unwrap();
    let validated = call(&config, "validate", json!({}));
    assert_eq!(validated, (true, json!({"valid":true})));
    let missing = call(&config, "declared-inspect", json!({"profile_id":"external-v1","idempotency_key":"one"}));
    assert!(!missing.0, "missing state must fail closed");
    assert!(!dir.path().join("state.json").exists());
    assert!(!dir.path().join("state.json.lock").exists());
    let capability_request = json!({"profile_id":"external-v1","producer_id":"test-external","schema":{"schema_ref":"manifest:test","schema_version":"1"}});
    assert!(!call(&config, "declared-capability", capability_request.clone()).0, "owner state must already be initialized");
    assert!(!dir.path().join("state.json").exists());
    assert!(!dir.path().join("state.json.lock").exists());
    // Initialize a real empty native view; capability inspection itself must
    // neither initialize state nor issue an admission for an arbitrary key.
    assert!(call(&config, "scoped", json!({})).0);
    let empty_state = fs::read(dir.path().join("state.json")).unwrap();
    let (ok, capability) = call(&config, "declared-capability", capability_request.clone());
    assert!(ok, "{capability}");
    assert_eq!(capability["response"]["kind"], "declared-capability");
    assert_eq!(capability["response"]["payload"]["data_view_id"], "test-view");
    assert_eq!(capability["response"]["payload"]["policy"], document["declared_policies"][0]);
    assert_eq!(fs::read(dir.path().join("state.json")).unwrap(), empty_state);
    for key in ["profile_id", "producer_id", "schema"] {
        let mut mismatch = capability_request.clone();
        mismatch[key] = if key == "schema" { json!({"schema_ref":"other","schema_version":"1"}) } else { json!("not-installed") };
        let (ok, denied) = call(&config, "declared-capability", mismatch);
        assert!(ok, "{denied}");
        assert!(denied["response"]["payload"].is_null());
        assert_eq!(fs::read(dir.path().join("state.json")).unwrap(), empty_state);
    }
    let (ok, issued) = call(
        &config,
        "admission-issue",
        json!({"profile_id":"external-v1","idempotency_key":"one","ttl_ms":60000}),
    );
    assert!(ok, "{issued}");
    assert_eq!(issued["response"]["kind"], "declared-admission");
    let admission = issued["response"]["payload"].clone();
    assert_eq!(capability["response"]["payload"]["policy_digest"], admission["admission_contract_digest"]);
    let input = json!({"submission_id":"one","admission":admission,"manifest":{"external_ref":"file:///external/test.json","external_revision":"product:one","digest_algorithm":"sha256","digest":"a".repeat(64),"size_bytes":900000,
      "schema":{"schema_ref":"manifest:test","schema_version":"1"},"media_type":"application/json","producer_id":"test-external","producer_version":"1","lineage_refs":["evidence:input"],"evidence_refs":["evidence:output"],"validation_refs":["evidence:validation"],
      "observed_or_produced_at_ms":source_kit::source_now_ms(),"policy_labels":["candidate"],"freshness":"fresh","retention":{"mode":"candidate-cleanup"}},"derivation":{"derivation_kind":"external-workflow","dependency_refs":[]}});
    let (ok, accepted) = call(&config, "declared-submit", input.clone());
    assert!(ok, "{accepted}");
    assert_eq!(
        accepted["response"]["payload"]["status"], "accepted",
        "{accepted}"
    );
    assert_eq!(call(&config, "declared-submit", input.clone()).1, accepted);
    let inspection = json!({"profile_id":"external-v1","idempotency_key":"one"});
    let before_inspection = fs::read(dir.path().join("state.json")).unwrap();
    // Public read-only inspection neither takes the writer lock nor persists
    // genesis, projection aliases, actions, renewed contexts or new outcomes.
    let writer = data_view::FileDataViewPersistence::new_exclusive(dir.path().join("state.json")).unwrap();
    assert_eq!(call(&config, "declared-capability", capability_request.clone()), (true, capability.clone()));
    let (ok, inspected) = call(&config, "declared-inspect", inspection.clone());
    assert!(ok, "{inspected}");
    assert_eq!(inspected["response"]["kind"], "declared-outcome");
    assert_eq!(inspected["response"]["payload"], accepted["response"]["payload"]);
    drop(writer);
    assert_eq!(fs::read(dir.path().join("state.json")).unwrap(), before_inspection);
    let unknown = call(&config, "declared-inspect", json!({"profile_id":"external-v1","idempotency_key":"missing"}));
    assert_eq!(unknown, (true, json!({"adapter":"fustor.native.dataview.v1","wireEncoding":"msgpack","response":{"kind":"declared-outcome","payload":null}})));
    for patch in ["authority", "labels", "epoch", "policy", "producer"] {
        let mut changed = document.clone();
        match patch {
            "authority" => changed["scoped_request"]["access"]["permitted_source_authority_refs"] = json!([]),
            "labels" => changed["scoped_request"]["access"]["visible_policy_labels"] = json!([]),
            "epoch" => changed["scoped_request"]["access"]["current_grant_epoch"] = json!(2),
            "policy" => changed["declared_policies"][0]["policy_context_digest"] = json!("another-policy"),
            "producer" => changed["declared_policies"][0]["producer_id"] = json!("another-producer"),
            _ => unreachable!(),
        }
        fs::write(&config, changed.to_string()).unwrap();
        assert_eq!(call(&config, "declared-inspect", inspection.clone()), unknown, "{patch}");
        if ["authority", "labels", "epoch"].contains(&patch) {
            // Viewer disclosure cannot grant or revoke the independently
            // installed producer's write capability. No write is performed.
            assert_eq!(call(&config, "declared-capability", capability_request.clone()), (true, capability.clone()), "{patch}");
        }
        assert_eq!(fs::read(dir.path().join("state.json")).unwrap(), before_inspection, "{patch}");
    }
    fs::write(&config, document.to_string()).unwrap();
    let mut revoked = document.clone();
    revoked["declared_policies"] = json!([]);
    fs::write(&config, revoked.to_string()).unwrap();
    let revoked_capability = call(&config, "declared-capability", capability_request.clone());
    assert!(revoked_capability.0);
    assert!(revoked_capability.1["response"]["payload"].is_null());
    assert_eq!(fs::read(dir.path().join("state.json")).unwrap(), before_inspection);
    let mut revised = document.clone();
    revised["declared_policies"][0]["grant_epoch"] = json!(2);
    fs::write(&config, revised.to_string()).unwrap();
    let revised_capability = call(&config, "declared-capability", capability_request).1;
    assert_ne!(revised_capability["response"]["payload"]["policy_digest"], capability["response"]["payload"]["policy_digest"]);
    assert_eq!(revised_capability["response"]["payload"]["policy"]["grant_epoch"], 2);
    assert_eq!(fs::read(dir.path().join("state.json")).unwrap(), before_inspection);
    fs::write(&config, document.to_string()).unwrap();
    let retry_admission = call(
        &config,
        "admission-issue",
        json!({"profile_id":"external-v1","idempotency_key":"one","ttl_ms":60000}),
    )
    .1;
    assert_eq!(retry_admission["response"]["payload"], admission);
    let (ok, scoped) = call(&config, "scoped", json!({"unrestricted":true}));
    assert!(ok, "{scoped}");
    let results = scoped["response"]["payload"]["projected"]["visible_results"]
        .as_array()
        .unwrap_or_else(|| panic!("{scoped}"));
    assert_eq!(results.len(), 1, "{scoped}");
    assert_eq!(
        results[0]["properties"]["external_ref"],
        "file:///external/test.json"
    );
    assert!(fs::metadata(dir.path().join("state.json")).unwrap().len() < 40000); // no referenced product bytes copied
    let before = fs::read(dir.path().join("state.json")).unwrap();
    let mut forged = input;
    forged["manifest"]["relationship_assertions"] = json!([{ "kind":"caller-injected" }]);
    assert!(!call(&config, "declared-submit", forged).0);
    assert_eq!(fs::read(dir.path().join("state.json")).unwrap(), before);
    document["scoped_request"]["access"]["permitted_source_authority_refs"] = json!([]);
    fs::write(&config, document.to_string()).unwrap();
    let denied = call(&config, "scoped", json!({})).1;
    assert_eq!(
        denied["response"]["payload"]["projected"]["visible_results"],
        json!([])
    );
    document["scoped_request"]["access"]["permitted_source_authority_refs"] = json!(["declared:test"]);
    document["declared_policies"][0]["policy_context_digest"] = json!("new-policy");
    fs::write(&config, document.to_string()).unwrap();
    assert!(call(&config, "admission-issue", json!({"profile_id":"external-v1","idempotency_key":"one","ttl_ms":60000})).0);
    let before = fs::read(dir.path().join("state.json")).unwrap();
    assert_eq!(call(&config, "declared-inspect", inspection), unknown,
               "a new admission context cannot relabel a historical outcome under another policy");
    assert_eq!(fs::read(dir.path().join("state.json")).unwrap(), before);
}
