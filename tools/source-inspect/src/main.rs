use capanix_app_sdk::{
    Event,
    runtime::{ConfigValue, EventMetadata, NodeId},
};
use csv_source::{
    CsvSourceRuntimeConfig,
    shared_types::{CsvSourcePayload, CsvSourceRequest},
};
use csv_source_runtime::{CsvSourceService, NativeCsvSourceDriver};
use es_source::{
    EsSourceRuntimeConfig,
    shared_types::{EsSourcePayload, EsSourceRequest},
};
use es_source_runtime::{EsSourceService, HttpEsSourceDriver};
use graph_source::{
    GraphSourcePayload, GraphSourceRequest, GraphSourceRuntimeConfig, GraphSourceService,
    NativeGraphSourceDriver,
};
use jsonl_source::{
    JsonlSourceRuntimeConfig,
    shared_types::{JsonlSourcePayload, JsonlSourceRequest},
};
use jsonl_source_runtime::{JsonlSourceService, NativeJsonlSourceDriver};
use local_fs_source::{
    LocalFsSourceRuntimeConfig,
    shared_types::{LocalFsSourcePayload, LocalFsSourceRequest},
};
use local_fs_source_runtime::{LocalFsSourceService, NativeLocalFsSourceDriver};
use mysql_source::{
    MysqlSourceRuntimeConfig,
    shared_types::{MysqlSourcePayload, MysqlSourceRequest},
};
use mysql_source_runtime::{MysqlSourceService, NativeMysqlSourceDriver};
use pg_source::{
    PgSourceRuntimeConfig,
    shared_types::{PgSourcePayload, PgSourceRequest},
};
use pg_source_runtime::{NativePgSourceDriver, PgSourceService};
use s3_source::{
    S3SourceRuntimeConfig,
    shared_types::{S3SourcePayload, S3SourceRequest},
};
use s3_source_runtime::{NativeS3SourceDriver, S3SourceService};
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use source_kit::{
    SourceExecutionEnvelope, SourcePayloadMetadata, decode_source_response, estimate_source_budget,
    source_now_ms,
};
use std::{
    collections::HashMap,
    io::{self, Read, Write},
    sync::Arc,
};
mod scope;

fn input() -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut raw = Vec::new();
    io::stdin()
        .take(2 * 1024 * 1024 + 1)
        .read_to_end(&mut raw)?;
    if raw.len() > 2 * 1024 * 1024 {
        return Err("input exceeds 2 MiB".into());
    }
    Ok(raw)
}
fn event<T: Serialize>(
    envelope: &SourceExecutionEnvelope<T>,
) -> Result<Event, Box<dyn std::error::Error>> {
    envelope.context.validate(source_now_ms())?;
    if envelope.wire_version != source_kit::SOURCE_WIRE_VERSION {
        return Err("unsupported wire version".into());
    }
    Ok(Event::new(
        EventMetadata {
            origin_id: NodeId("source-inspect".into()),
            timestamp_us: source_now_ms() * 1000,
            logical_ts: None,
            correlation_id: None,
            ingress_auth: None,
            trace: None,
        },
        rmp_serde::to_vec_named(envelope)?.into(),
    ))
}
fn output<P: DeserializeOwned + Serialize + SourcePayloadMetadata>(
    reply: Event,
    wire: bool,
    configuration_revision: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let envelope = match decode_source_response::<P>(reply.payload_bytes())? {
        source_kit::DecodedSourceResponse::Envelope(value) => value,
        source_kit::DecodedSourceResponse::Legacy(_) => {
            return Err("native reader did not return a versioned response".into());
        }
    };
    if wire {
        io::stdout().lock().write_all(reply.payload_bytes())?;
        return Ok(());
    }
    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({
            "adapter":"fustor.native.oneshot.v1","wireEncoding":"msgpack",
            "configurationRevision":configuration_revision,
            "continuationUsable":false,"sessionLifecycle":"closed-on-exit","response":envelope
        }))?
    );
    Ok(())
}

// Dispatch into source owners, never a second connector implementation. Each invocation
// destroys the registry (including MySQL/PG snapshots and Elasticsearch PITs) before reply.
macro_rules! inspect {
    ($cfg:expr, $mode:expr, $revision:expr, $config:ty, $request:ty, $payload:ty, $service:ty, $driver:expr) => {{
        let config = <$config>::from_manifest_config($cfg)?;
        if $mode == "validate" {
            println!("{{\"valid\":true}}");
        } else {
            let service = <$service>::new(config, Arc::new($driver), NodeId("source-inspect".into()));
            if $mode == "describe" {
                let request = Event::new(EventMetadata {
                    origin_id: NodeId("source-inspect".into()), timestamp_us: source_now_ms() * 1000,
                    logical_ts: None, correlation_id: None, ingress_auth: None, trace: None,
                }, Vec::<u8>::new().into());
                let reply = service.handle_status_events(vec![request], "oneshot.fetch", "oneshot.status").remove(0);
                let value: $payload = rmp_serde::from_slice(reply.payload_bytes())?;
                println!("{}", serde_json::json!({"adapter":"fustor.native.oneshot.v1", "status":value, "configurationRevision":$revision, "continuationUsable":false}));
            } else {
                let request: SourceExecutionEnvelope<$request> = serde_json::from_slice(&input()?)?;
                if request.request.cursor.is_some() || request.request.limit.is_none() {
                    return Err("one-shot read requires explicit limit and no continuation".into());
                }
                let reply = service.handle_event(event(&request)?);
                drop(service);
                output::<$payload>(reply, $mode == "read-wire", $revision)?;
            }
        }
    }};
}
fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().is_some_and(|s| s == "--help") || args.is_empty() {
        println!(
            "fustor-source-inspect budget < request.json\nfustor-source-inspect read|read-wire|validate|describe --kind jsonl|csv|local-fs|s3|mysql|es|pg|graph --config native.json < envelope.json\nfustor-source-inspect validate-scope --kind <kind> --config - < proposal.json"
        );
        return Ok(());
    }
    if args == ["budget"] {
        let request = serde_json::from_slice(&input()?)?;
        println!(
            "{}",
            serde_json::to_string(&estimate_source_budget(&request)?)?
        );
        return Ok(());
    }
    if args.len() != 5
        || args[1] != "--kind"
        || args[3] != "--config"
        || ![
            "read",
            "read-wire",
            "validate",
            "validate-scope",
            "describe",
        ]
        .contains(&args[0].as_str())
    {
        return Err("invalid command; see --help".into());
    }
    if args[0] == "validate-scope" {
        if args[4] != "-" {
            return Err("scope validation requires a stdin proposal".into());
        }
        return scope::validate(&args[2], &input()?);
    }
    let mut config_bytes = Vec::new();
    if args[0] == "validate" && args[4] == "-" {
        config_bytes = input()?;
    } else {
        let file = std::fs::File::open(&args[4])?;
        file.take(2 * 1024 * 1024 + 1)
            .read_to_end(&mut config_bytes)?;
    }
    if config_bytes.len() > 2 * 1024 * 1024 {
        return Err("configuration exceeds 2 MiB".into());
    }
    let cfg: HashMap<String, ConfigValue> = serde_yaml::from_slice(&config_bytes)?;
    let configuration_revision = format!("{:x}", Sha256::digest(&config_bytes));
    match args[2].as_str() {
        "jsonl" => inspect!(
            &cfg,
            &args[0],
            &configuration_revision,
            JsonlSourceRuntimeConfig,
            JsonlSourceRequest,
            JsonlSourcePayload,
            JsonlSourceService,
            NativeJsonlSourceDriver
        ),
        "csv" => inspect!(
            &cfg,
            &args[0],
            &configuration_revision,
            CsvSourceRuntimeConfig,
            CsvSourceRequest,
            CsvSourcePayload,
            CsvSourceService,
            NativeCsvSourceDriver
        ),
        "pg" => inspect!(
            &cfg,
            &args[0],
            &configuration_revision,
            PgSourceRuntimeConfig,
            PgSourceRequest,
            PgSourcePayload,
            PgSourceService,
            NativePgSourceDriver
        ),
        "graph" => inspect!(
            &cfg,
            &args[0],
            &configuration_revision,
            GraphSourceRuntimeConfig,
            GraphSourceRequest,
            GraphSourcePayload,
            GraphSourceService,
            NativeGraphSourceDriver
        ),
        "mysql" => inspect!(
            &cfg,
            &args[0],
            &configuration_revision,
            MysqlSourceRuntimeConfig,
            MysqlSourceRequest,
            MysqlSourcePayload,
            MysqlSourceService,
            NativeMysqlSourceDriver
        ),
        "es" => inspect!(
            &cfg,
            &args[0],
            &configuration_revision,
            EsSourceRuntimeConfig,
            EsSourceRequest,
            EsSourcePayload,
            EsSourceService,
            HttpEsSourceDriver::default()
        ),
        "local-fs" => inspect!(
            &cfg,
            &args[0],
            &configuration_revision,
            LocalFsSourceRuntimeConfig,
            LocalFsSourceRequest,
            LocalFsSourcePayload,
            LocalFsSourceService,
            NativeLocalFsSourceDriver
        ),
        "s3" => inspect!(
            &cfg,
            &args[0],
            &configuration_revision,
            S3SourceRuntimeConfig,
            S3SourceRequest,
            S3SourcePayload,
            S3SourceService,
            NativeS3SourceDriver
        ),
        _ => return Err("unsupported source kind".into()),
    }
    Ok(())
}
fn main() {
    if let Err(error) = run() {
        let code = if std::env::args().nth(1).as_deref() == Some("validate-scope") {
            "source-scope-invalid"
        } else {
            "source-inspect-error"
        };
        eprintln!(
            "{}",
            serde_json::json!({"ok":false,"errorCode":code,"message":error.to_string()})
        );
        std::process::exit(2);
    }
}
