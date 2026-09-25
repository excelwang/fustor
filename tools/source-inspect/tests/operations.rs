// ASSERTION INTENT (Black-box tests — public traits and APIs only). Do not introduce white-box testing logic or internal workarounds.
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use source_kit::{
    AccessUsage, BudgetEstimateRequest, BudgetOverrides, SourceExecutionLimits,
    SourceObservationReceipt, estimate_source_budget,
};
use std::{
    fs,
    io::Write,
    process::{Command, Stdio},
};

fn limits() -> SourceExecutionLimits {
    SourceExecutionLimits {
        max_rows: 10,
        max_bytes: 262144,
        max_calls: 3,
        max_fanout: 1,
        max_latency_ms: 5000,
        max_cost_units: 3,
    }
}
fn sample(at: u64, bytes: u64, latency: u64) -> AccessUsage {
    let receipt:SourceObservationReceipt=serde_json::from_value(json!({
        "source_kind":"jsonl","authority_object_ref":"source","operation":"snapshot-page","source_revision":"revision:1",
        "native_anchors":[],"evidence_refs":[],"observed_at_ms":at,"grant_epoch":1,"continuation_state":"terminal",
        "actual":{"rows":2,"bytes":bytes,"latency_ms":latency,"calls":1,"fanout":1,"cost_units":1},
        "payload_digest":"native-payload","receipt_digest":"native-receipt"
    })).unwrap();
    AccessUsage {
        context_key: "scope-and-revision".into(),
        observed_at_ms: at,
        outcome: "success".into(),
        allocated: limits(),
        receipt: Some(receipt),
        truncated: false,
    }
}
fn request() -> BudgetEstimateRequest {
    let mut cap = limits();
    cap.max_latency_ms = 10000;
    cap.max_bytes = 1048576;
    BudgetEstimateRequest {
        now_ms: 100_000_000,
        context_key: "scope-and-revision".into(),
        defaults: limits(),
        hard_caps: vec![cap],
        history: vec![],
        overrides: BudgetOverrides::default(),
    }
}
// @verify_spec("CONTRACTS.OPERATIONS.PASSIVE_BUDGET_ESTIMATE", mode="system")
#[test]
fn budget_cold_warm_caps_override_and_context_are_passive() {
    let mut input = request();
    let cold = estimate_source_budget(&input).unwrap();
    assert_eq!(cold.limits, input.defaults);
    input.history = (0..10)
        .map(|i| sample(input.now_ms - i, 100_000, 2000))
        .collect();
    let warm = estimate_source_budget(&input).unwrap();
    assert_eq!(warm.limits.max_bytes, 150_000);
    assert_eq!(warm.limits.max_latency_ms, 3000);
    assert_eq!(warm.limits.max_rows, input.defaults.max_rows);
    assert_eq!(warm.limits.max_calls, 3);
    input.overrides.max_bytes = Some(65536);
    assert_eq!(
        estimate_source_budget(&input).unwrap().max_bytes.mode,
        "manual"
    );
    input.overrides.max_bytes = Some(2_000_000);
    assert!(estimate_source_budget(&input).is_err());
    input.overrides = BudgetOverrides::default();
    input.context_key = "new-grant".into();
    assert_eq!(
        estimate_source_budget(&input).unwrap().limits,
        input.defaults
    );
}
// @verify_spec("CONTRACTS.OPERATIONS.PASSIVE_BUDGET_ESTIMATE", mode="system")
#[test]
fn history_window_cardinality_exhaustion_and_non_training_failures() {
    let mut input = request();
    input.history = (0..110)
        .map(|i| sample(input.now_ms - i, 100_000 + i, 2000))
        .collect();
    assert_eq!(
        estimate_source_budget(&input)
            .unwrap()
            .max_bytes
            .sample_count,
        100
    );
    input.history = vec![sample(input.now_ms - 86_400_000, 100, 100)];
    assert_eq!(
        estimate_source_budget(&input)
            .unwrap()
            .max_bytes
            .sample_count,
        0
    );
    let mut failure = sample(input.now_ms, 0, 0);
    failure.receipt = None;
    failure.outcome = "bytes-exhausted".into();
    input.history = vec![failure.clone()];
    assert_eq!(
        estimate_source_budget(&input).unwrap().limits.max_bytes,
        524288
    );
    input.hard_caps[0].max_bytes = 300000;
    let capped = estimate_source_budget(&input).unwrap();
    assert_eq!(capped.limits.max_bytes, 300000);
    assert!(capped.max_bytes.insufficient);
    failure.outcome = "network".into();
    input.history = vec![failure];
    assert_eq!(
        estimate_source_budget(&input).unwrap().limits.max_bytes,
        input.defaults.max_bytes
    );
    input.hard_caps[0].max_rows = 1;
    assert!(estimate_source_budget(&input).is_err());
}
fn call(args: &[&str], value: Value) -> (bool, Value) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_fustor-source-inspect"))
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(value.to_string().as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    let raw = if output.status.success() {
        &output.stdout
    } else {
        &output.stderr
    };
    (
        output.status.success(),
        serde_json::from_slice(raw).unwrap(),
    )
}
// @verify_spec("CONTRACTS.OPERATIONS.ONE_SHOT_INSPECTION", mode="system")
#[test]
fn native_jsonl_cli_enforces_bounds_grants_and_receipts() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("sample.jsonl"), "{\"id\":1}\n{\"id\":2}\n").unwrap();
    let config = dir.path().join("native.json");
    fs::write(&config,json!({
        "default_page_size":1,"max_page_size":10,"field_sample_limit":2,
        "endpoints":[{"object_ref":"source","root_path":dir.path(),"path_scopes":["sample.jsonl"],"active":true}],
        "__cnx_runtime":{"resource_grants":[{"resource_kind":"jsonl","object_ref":"source","interfaces":["read"],"path_scopes":["sample.jsonl"],"grant_epoch":1,"grant_state":"active"}]}
    }).to_string()).unwrap();
    let now = source_kit::source_now_ms();
    let mut input = json!({"wire_version":2,"context":{"request_id":"inspect","grant_epoch":1,"issued_at_ms":now,"deadline_ms":now+5000,"limits":limits()},
        "request":{"object_ref":"source","operation":"snapshot-page","path":"sample.jsonl","limit":1}});
    let args = [
        "read",
        "--kind",
        "jsonl",
        "--config",
        config.to_str().unwrap(),
    ];
    let (ok, result) = call(&args, input.clone());
    assert!(ok, "{result}");
    assert_eq!(
        result["configurationRevision"],
        format!("{:x}", Sha256::digest(fs::read(&config).unwrap()))
    );
    assert_eq!(result["response"]["receipt"]["actual"]["rows"], 1);
    assert!(
        result["response"]["receipt"]["actual"]["bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(result["continuationUsable"], false);
    input["context"]["grant_epoch"] = json!(2);
    let (_, denied) = call(&args, input.clone());
    assert_eq!(denied["response"]["payload"]["kind"], "error");
    assert!(denied["response"]["receipt"].is_null());
    input["context"]["grant_epoch"] = json!(1);
    input["request"]["path"] = json!("../secret.jsonl");
    let (_, denied) = call(&args, input);
    assert_eq!(denied["response"]["payload"]["kind"], "error");
}

// @verify_spec("CONTRACTS.INSPECTION.NATIVE_INSPECTION_PARITY", mode="system")
#[test]
fn native_file_content_is_distinct_from_metadata_and_bounded() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("sample.jsonl"), "{\"id\":1}\n{\"id\":2}\n").unwrap();
    fs::write(
        dir.path().join("sample.csv"),
        "id,title\n1,\"two\nlines\"\n2,other\n",
    )
    .unwrap();
    fs::write(dir.path().join("bad.jsonl"), "{broken}\n").unwrap();
    fs::write(
        dir.path().join("huge.jsonl"),
        format!("{{\"text\":\"{}\"}}\n", "x".repeat(300000)),
    )
    .unwrap();
    let config = dir.path().join("native.json");
    fs::write(&config, json!({"default_page_size":2,"max_page_size":10,
        "endpoints":[{"object_ref":"files","root_path":dir.path(),"path_scopes":["*.jsonl","*.csv"],"active":true}],
        "__cnx_runtime":{"resource_grants":[{"resource_kind":"local-fs","object_ref":"files","interfaces":["read"],"path_scopes":["*.jsonl","*.csv"],"grant_epoch":1,"grant_state":"active"}]}
    }).to_string()).unwrap();
    let args = [
        "read",
        "--kind",
        "local-fs",
        "--config",
        config.to_str().unwrap(),
    ];
    let now = source_kit::source_now_ms();
    let mut request = json!({"wire_version":2,"context":{"request_id":"files","grant_epoch":1,"issued_at_ms":now,"deadline_ms":now+5000,"limits":limits()},
        "request":{"object_ref":"files","operation":"sample-jsonl","path":"sample.jsonl","limit":2}});
    let (ok, result) = call(&args, request.clone());
    assert!(ok, "{result}");
    assert_eq!(result["response"]["payload"]["kind"], "content", "{result}");
    assert_eq!(result["response"]["payload"]["payload"]["rows"][0]["id"], 1);
    assert_eq!(result["response"]["receipt"]["operation"], "sample-jsonl");
    assert_eq!(result["continuationUsable"], false);
    request["request"]["operation"] = json!("sample-csv");
    request["request"]["path"] = json!("sample.csv");
    let (_, result) = call(&args, request.clone());
    assert_eq!(
        result["response"]["payload"]["payload"]["rows"][0]["title"], "two\nlines",
        "{result}"
    );
    request["request"]["operation"] = json!("sample-jsonl");
    for denied in ["bad.jsonl", "huge.jsonl", "../outside.jsonl"] {
        request["request"]["path"] = json!(denied);
        let (_, result) = call(&args, request.clone());
        assert_eq!(result["response"]["payload"]["kind"], "error", "{result}");
        assert!(result["response"]["receipt"].is_null());
    }
    let (ok, described) = call(
        &[
            "describe",
            "--kind",
            "local-fs",
            "--config",
            config.to_str().unwrap(),
        ],
        json!({}),
    );
    assert!(ok, "{described}");
    assert!(
        described["status"]["payload"]["capabilities"]["operations"]
            .as_array()
            .unwrap()
            .contains(&json!("bounded-jsonl-content"))
    );
}
