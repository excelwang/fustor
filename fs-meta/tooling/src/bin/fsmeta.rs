//! fs-meta product CLI.
//!
//! This binary is a product-scoped operator client only. It parses operator
//! workflows, submits typed runtime-admin or HTTP requests, and presents typed
//! responses. It does not own `link`, `bind/run` realization, `route`
//! convergence, `route` target selection, or `state/effect observation plane`
//! meaning such as `observation_eligible`.
//! Deploy/cutover requests submit a new release generation through the product
//! boundary only; trusted external observation remains gated by app-owned
//! `observation_eligible` after replay and rebuild.

use std::fs;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use clap::{Args, Parser, Subcommand};
use fs_meta::RootSpec as RootEntry;
use fs_meta::api::{ApiAuthConfig, BootstrapManagementConfig};
use fs_meta_deploy::{
    FsMetaReleaseSpec, FsMetaReleaseWorkerMode, FsMetaReleaseWorkerModes, build_release_doc_value,
    compile_release_doc_to_relation_target_intent, write_startup_manifest,
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "fsmeta", version, about = "Independent fs-meta product CLI")]
struct Cli {
    #[arg(long, default_value = "human")]
    output: String,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Deploy(DeployArgs),
    Undeploy(UndeployArgs),
    Local {
        #[command(subcommand)]
        command: LocalCommand,
    },
    Grants {
        #[command(subcommand)]
        command: GrantsCommand,
    },
    Roots {
        #[command(subcommand)]
        command: RootsCommand,
    },
}

#[derive(Args, Debug)]
struct ControlAuthArgs {
    #[arg(long)]
    socket: Option<String>,
    #[arg(long)]
    actor_id: Option<String>,
    #[arg(long, default_value = "local")]
    domain_id: String,
    #[arg(long, default_value = "local-admin-ed25519-1")]
    key_id: String,
    #[arg(long, env = "CAPANIX_CTL_SK_B64")]
    admin_sk_b64: Option<String>,
}

#[derive(Args, Debug)]
struct DeployArgs {
    #[command(flatten)]
    control: ControlAuthArgs,
    #[arg(long)]
    config: PathBuf,
}

#[derive(Args, Debug)]
struct UndeployArgs {
    #[command(flatten)]
    control: ControlAuthArgs,
    #[arg(long)]
    app_id: String,
}

#[derive(Subcommand, Debug)]
enum LocalCommand {
    Start(LocalStartArgs),
    Stop(LocalWorkdirArgs),
    Status(LocalWorkdirArgs),
}

#[derive(Args, Debug)]
struct LocalStartArgs {
    #[arg(long)]
    workdir: PathBuf,
    #[arg(long)]
    config: PathBuf,
}

#[derive(Args, Debug)]
struct LocalWorkdirArgs {
    #[arg(long)]
    workdir: PathBuf,
}

#[derive(Subcommand, Debug)]
enum GrantsCommand {
    List(ApiAuthArgs),
}

#[derive(Subcommand, Debug)]
enum RootsCommand {
    Preview(RootsPreviewArgs),
    Apply(RootsApplyArgs),
}

#[derive(Args, Debug, Clone)]
struct ApiAuthArgs {
    #[arg(long)]
    api_base: String,
    #[arg(long, env = "FSMETA_API_TOKEN")]
    token: Option<String>,
    #[arg(long)]
    username: Option<String>,
    #[arg(long)]
    password: Option<String>,
    #[arg(long)]
    password_file: Option<PathBuf>,
}

#[derive(Args, Debug)]
struct RootsPreviewArgs {
    #[command(flatten)]
    api: ApiAuthArgs,
    #[arg(long)]
    file: PathBuf,
}

#[derive(Args, Debug)]
struct RootsApplyArgs {
    #[command(flatten)]
    api: ApiAuthArgs,
    #[command(flatten)]
    control: ControlAuthArgs,
    #[arg(long)]
    file: PathBuf,
    #[arg(long)]
    config: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionLoginResponse {
    token: String,
    expires_in_secs: u64,
    user: serde_json::Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProductConfig {
    api: ProductApiConfig,
    #[serde(default)]
    auth: ProductAuthConfig,
    #[serde(default)]
    workers: ProductWorkersConfig,
    #[serde(default)]
    scan_workers: Option<u64>,
    #[serde(default)]
    max_scan_events: Option<u64>,
    #[serde(default)]
    audit_interval_ms: Option<u64>,
    #[serde(default)]
    throttle_interval_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ProductApiConfig {
    facade_resource_id: String,
}

#[derive(Debug, Default, Deserialize)]
struct ProductAuthConfig {
    #[serde(default)]
    bootstrap_management: Option<ProductBootstrapAdmin>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProductWorkersConfig {
    #[serde(default)]
    facade: Option<ProductWorkerConfig>,
    #[serde(default)]
    source: Option<ProductWorkerConfig>,
    #[serde(default)]
    sink: Option<ProductWorkerConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProductWorkerConfig {
    #[serde(default)]
    mode: Option<FsMetaReleaseWorkerMode>,
}

#[derive(Debug, Clone, Deserialize)]
struct ProductBootstrapAdmin {
    #[serde(default = "default_bootstrap_username")]
    username: String,
    #[serde(default)]
    password: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LocalRuntimeState {
    pid: u32,
    api_base_url: String,
    username: String,
    password: String,
    started_at_us: u64,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RootsFile {
    Wrapped { roots: Vec<RootEntry> },
    Bare(Vec<RootEntry>),
}

const DEFAULT_APP_ID: &str = "fs-meta";

fn default_local_ingress_bind_addr() -> String {
    "127.0.0.1:19180".to_string()
}

fn default_bootstrap_username() -> String {
    "fsmeta-admin".to_string()
}

fn configured_worker_mode(config: &Option<ProductWorkerConfig>) -> Option<FsMetaReleaseWorkerMode> {
    config.as_ref().and_then(|config| config.mode)
}

fn resolve_release_worker_modes(
    product: &ProductConfig,
) -> anyhow::Result<FsMetaReleaseWorkerModes> {
    let facade = configured_worker_mode(&product.workers.facade)
        .unwrap_or(FsMetaReleaseWorkerMode::Embedded);
    if facade != FsMetaReleaseWorkerMode::Embedded {
        bail!("fs-meta product config requires workers.facade.mode=embedded");
    }

    Ok(FsMetaReleaseWorkerModes {
        facade,
        source: configured_worker_mode(&product.workers.source)
            .unwrap_or(FsMetaReleaseWorkerMode::External),
        sink: configured_worker_mode(&product.workers.sink)
            .unwrap_or(FsMetaReleaseWorkerMode::External),
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let value = match cli.command {
        Commands::Deploy(args) => deploy(args).await?,
        Commands::Undeploy(args) => undeploy(args).await?,
        Commands::Local { command } => match command {
            LocalCommand::Start(args) => local_start(args).await?,
            LocalCommand::Stop(args) => local_stop(args).await?,
            LocalCommand::Status(args) => local_status(args).await?,
        },
        Commands::Grants { command } => match command {
            GrantsCommand::List(args) => grants_list(args).await?,
        },
        Commands::Roots { command } => match command {
            RootsCommand::Preview(args) => roots_preview(args).await?,
            RootsCommand::Apply(args) => roots_apply(args).await?,
        },
    };
    let rendered = serde_json::to_string_pretty(&value)?;
    match cli.output.trim().to_ascii_lowercase().as_str() {
        "human" | "json" => println!("{rendered}"),
        other => bail!("unsupported --output value: {other}"),
    }
    Ok(())
}

async fn deploy(args: DeployArgs) -> anyhow::Result<serde_json::Value> {
    let product = load_product_config(&args.config)?;
    let state_dir = prepare_state_dir(args.config.parent(), ".fsmeta-state")?;
    let (auth_cfg, username, password) = build_auth_config(&product.auth, &state_dir);
    auth_cfg
        .materialize_bootstrap_management()
        .map_err(|err| anyhow::anyhow!("materialize bootstrap management auth failed: {err}"))?;
    let (app_target, manifest_path) = resolve_fs_meta_runtime_inputs()?;
    let control = ControlClient::from_args(&args.control)?;
    let route_plan_node_ids = control.discover_route_plan_node_ids().await?;
    let intent = build_deploy_intent(
        &product,
        auth_cfg,
        &app_target,
        &manifest_path,
        &state_dir,
        route_plan_node_ids.clone(),
    )?;
    let result = control.apply_relation_target_intent(&intent).await?;
    Ok(json!({
        "status": "ok",
        "app_id": DEFAULT_APP_ID,
        "api_facade_resource_id": product.api.facade_resource_id,
        "route_plan_node_ids": route_plan_node_ids,
        "bootstrap_management": {
            "username": username,
            "password": password,
        },
        "state_dir": state_dir,
        "result": result,
    }))
}

async fn undeploy(args: UndeployArgs) -> anyhow::Result<serde_json::Value> {
    let control = ControlClient::from_args(&args.control)?;
    let result = control.clear_relation_target(args.app_id.clone()).await?;
    Ok(json!({
        "status": "ok",
        "app_id": args.app_id,
        "result": result,
    }))
}

async fn local_start(args: LocalStartArgs) -> anyhow::Result<serde_json::Value> {
    fs::create_dir_all(&args.workdir)?;
    let state_path = args.workdir.join("runtime.json");
    if state_path.exists() {
        let state = load_local_state(&state_path)?;
        if pid_is_running(state.pid) {
            bail!(
                "local runtime already running for workdir {}",
                args.workdir.display()
            );
        }
    }

    let product = load_product_config(&args.config)?;
    let (auth_cfg, username, password) = build_auth_config(&product.auth, &args.workdir);
    auth_cfg
        .materialize_bootstrap_management()
        .map_err(|err| anyhow::anyhow!("materialize local bootstrap auth failed: {err}"))?;

    let facade_resource_id = product.api.facade_resource_id.clone();
    let bind_addr = default_local_ingress_bind_addr();
    let api_base_url = format!("http://{}", bind_addr);
    let stdout_log = args.workdir.join("server.stdout.log");
    let stderr_log = args.workdir.join("server.stderr.log");
    let stdout = fs::File::create(&stdout_log)?;
    let stderr = fs::File::create(&stderr_log)?;
    let fixture_bin = ensure_fixture_binary()?;
    let roots_json = serde_json::to_string(&Vec::<RootEntry>::new())?;
    let mut cmd = Command::new(&fixture_bin);
    cmd.env("FS_META_API_FACADE_RESOURCE_ID", &facade_resource_id)
        .env("FS_META_API_LISTENER_BIND_ADDR", &bind_addr)
        .env("FS_META_ROOTS_JSON", roots_json)
        .env(
            "FS_META_PASSWD_PATH",
            auth_cfg.passwd_path.display().to_string(),
        )
        .env(
            "FS_META_SHADOW_PATH",
            auth_cfg.shadow_path.display().to_string(),
        )
        .env("FS_META_NODE_ID", "fsmeta-local")
        .current_dir(&args.workdir)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    unsafe {
        cmd.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = cmd
        .spawn()
        .with_context(|| format!("spawn {} failed", fixture_bin.display()))?;

    wait_for_local_ready(&api_base_url, &username, &password, &mut child).await?;

    let state = LocalRuntimeState {
        pid: child.id(),
        api_base_url: api_base_url.clone(),
        username: username.clone(),
        password: password.clone(),
        started_at_us: now_us(),
    };
    fs::write(&state_path, serde_json::to_string_pretty(&state)?)?;
    fs::write(
        args.workdir.join("credentials.json"),
        serde_json::to_string_pretty(&json!({
            "api_base_url": api_base_url,
            "username": username,
            "password": password,
        }))?,
    )?;

    Ok(json!({
        "status": "ok",
        "workdir": args.workdir,
        "pid": state.pid,
        "api_base_url": state.api_base_url,
        "bootstrap_management": {
            "username": state.username,
            "password": state.password,
        },
        "logs": {
            "stdout": stdout_log,
            "stderr": stderr_log,
        }
    }))
}

async fn local_stop(args: LocalWorkdirArgs) -> anyhow::Result<serde_json::Value> {
    let state_path = args.workdir.join("runtime.json");
    let state = load_local_state(&state_path)?;
    let mut stopped = false;
    if pid_is_running(state.pid) {
        let status = Command::new("kill")
            .arg(state.pid.to_string())
            .status()
            .context("runtime stop failed")?;
        if !status.success() {
            bail!("kill {} failed with status {}", state.pid, status);
        }
        stopped = true;
    }
    let _ = fs::remove_file(&state_path);
    Ok(json!({
        "status": "ok",
        "workdir": args.workdir,
        "pid": state.pid,
        "stopped": stopped,
    }))
}

async fn local_status(args: LocalWorkdirArgs) -> anyhow::Result<serde_json::Value> {
    let state = load_local_state(&args.workdir.join("runtime.json"))?;
    let running = pid_is_running(state.pid);
    let service_status = if running {
        let client = reqwest::Client::new();
        match login_api(
            &client,
            &state.api_base_url,
            &state.username,
            &state.password,
        )
        .await
        {
            Ok(token) => match client
                .get(format!("{}/api/fs-meta/v1/status", state.api_base_url))
                .bearer_auth(token)
                .send()
                .await
            {
                Ok(resp) => resp.json::<serde_json::Value>().await.ok(),
                Err(_) => None,
            },
            Err(_) => None,
        }
    } else {
        None
    };
    Ok(json!({
        "status": "ok",
        "running": running,
        "pid": state.pid,
        "api_base_url": state.api_base_url,
        "service_status": service_status,
    }))
}

async fn grants_list(args: ApiAuthArgs) -> anyhow::Result<serde_json::Value> {
    let client = reqwest::Client::new();
    let token = resolve_api_token(&client, &args).await?;
    get_json(
        &client,
        &args.api_base,
        "/api/fs-meta/v1/runtime/grants",
        &token,
    )
    .await
}

async fn roots_preview(args: RootsPreviewArgs) -> anyhow::Result<serde_json::Value> {
    let client = reqwest::Client::new();
    let token = resolve_api_token(&client, &args.api).await?;
    let payload = load_roots_payload(&args.file)?;
    post_json(
        &client,
        &args.api.api_base,
        "/api/fs-meta/v1/monitoring/roots/preview",
        &token,
        &payload,
    )
    .await
}

async fn roots_apply(args: RootsApplyArgs) -> anyhow::Result<serde_json::Value> {
    let client = reqwest::Client::new();
    let payload = load_roots_payload(&args.file)?;
    let roots = roots_from_payload(&payload)?;
    let runtime_apply = if let Some(config_path) = args.config.as_deref() {
        ensure_api_credentials_for_generation_cutover(&args.api)?;
        let preflight_token = resolve_api_login_token(&client, &args.api).await?;
        let preview = post_json(
            &client,
            &args.api.api_base,
            "/api/fs-meta/v1/monitoring/roots/preview",
            &preflight_token,
            &payload,
        )
        .await?;
        ensure_roots_preview_has_no_unmatched(&preview)?;
        let product = load_product_config(config_path)?;
        let state_dir = prepare_state_dir(config_path.parent(), ".fsmeta-state")?;
        let auth_cfg = load_deployed_auth_config(&state_dir)?;
        let (app_target, manifest_path) = resolve_fs_meta_runtime_inputs()?;
        let control = ControlClient::from_args(&args.control)?;
        let route_plan_node_ids = control.discover_route_plan_node_ids().await?;
        let intent = build_deploy_intent_for_roots(
            &product,
            auth_cfg,
            &app_target,
            &manifest_path,
            &state_dir,
            route_plan_node_ids.clone(),
            roots,
        )?;
        Some(json!({
            "route_plan_node_ids": route_plan_node_ids,
            "result": control.apply_relation_target_intent(&intent).await?,
        }))
    } else {
        None
    };
    let token = if args.config.is_some() {
        resolve_api_login_token(&client, &args.api).await?
    } else {
        resolve_api_token(&client, &args.api).await?
    };
    let roots_result = put_json(
        &client,
        &args.api.api_base,
        "/api/fs-meta/v1/monitoring/roots",
        &token,
        &payload,
    )
    .await?;
    match runtime_apply {
        Some(runtime_apply) => Ok(json!({
            "status": "ok",
            "roots_count": roots_result
                .get("roots_count")
                .and_then(serde_json::Value::as_u64),
            "runtime_apply": runtime_apply,
            "roots_apply": roots_result,
        })),
        None => Ok(roots_result),
    }
}

fn load_product_config(path: &Path) -> anyhow::Result<ProductConfig> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("read config {} failed", path.display()))?;
    let product = serde_yaml::from_str::<ProductConfig>(&content)
        .with_context(|| format!("parse config {} failed", path.display()))?;
    if product.api.facade_resource_id.trim().is_empty() {
        bail!("api.facade_resource_id must not be empty");
    }
    Ok(product)
}

fn build_auth_config(auth: &ProductAuthConfig, base_dir: &Path) -> (ApiAuthConfig, String, String) {
    let bootstrap = auth
        .bootstrap_management
        .clone()
        .unwrap_or(ProductBootstrapAdmin {
            username: default_bootstrap_username(),
            password: None,
        });
    let username = bootstrap.username;
    let password = bootstrap
        .password
        .unwrap_or_else(|| Uuid::new_v4().simple().to_string());
    (
        ApiAuthConfig {
            passwd_path: base_dir.join("fs-meta.passwd"),
            shadow_path: base_dir.join("fs-meta.shadow"),
            query_keys_path: base_dir.join("fs-meta.query-keys.json"),
            session_ttl_secs: 3600,
            management_group: "fsmeta_management".to_string(),
            bootstrap_management: Some(BootstrapManagementConfig {
                username: username.clone(),
                password: password.clone(),
                uid: 1000,
                gid: 1000,
                home: format!("/home/{username}"),
                shell: "/bin/bash".to_string(),
            }),
        },
        username,
        password,
    )
}

fn load_deployed_auth_config(state_dir: &Path) -> anyhow::Result<ApiAuthConfig> {
    let passwd_path = state_dir.join("fs-meta.passwd");
    let shadow_path = state_dir.join("fs-meta.shadow");
    if !passwd_path.exists() || !shadow_path.exists() {
        bail!(
            "deployed fs-meta auth files are missing under {}; run fsmeta deploy first or pass the deploy config whose parent owns .fsmeta-state",
            state_dir.display()
        );
    }
    Ok(ApiAuthConfig {
        passwd_path,
        shadow_path,
        query_keys_path: state_dir.join("fs-meta.query-keys.json"),
        session_ttl_secs: 3600,
        management_group: "fsmeta_management".to_string(),
        bootstrap_management: None,
    })
}

fn load_roots_payload(path: &Path) -> anyhow::Result<serde_json::Value> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("read roots file {} failed", path.display()))?;
    let file = serde_yaml::from_str::<RootsFile>(&content)
        .with_context(|| format!("parse roots file {} failed", path.display()))?;
    let roots = match file {
        RootsFile::Wrapped { roots } => roots,
        RootsFile::Bare(roots) => roots,
    };
    Ok(json!({ "roots": roots }))
}

fn roots_from_payload(payload: &serde_json::Value) -> anyhow::Result<Vec<RootEntry>> {
    let roots = payload
        .get("roots")
        .cloned()
        .context("roots payload missing roots")?;
    serde_json::from_value(roots).context("decode roots payload failed")
}

fn ensure_api_credentials_for_generation_cutover(args: &ApiAuthArgs) -> anyhow::Result<()> {
    if args.username.is_some() && (args.password.is_some() || args.password_file.is_some()) {
        return Ok(());
    }
    bail!(
        "roots apply --config performs a runtime generation cutover and requires --username with --password or --password-file so the CLI can re-login after relation_target_apply"
    )
}

fn ensure_roots_preview_has_no_unmatched(preview: &serde_json::Value) -> anyhow::Result<()> {
    let unmatched = preview
        .get("unmatched_roots")
        .or_else(|| preview.get("unmatched"))
        .and_then(serde_json::Value::as_array)
        .context("roots preview response missing unmatched_roots array")?;
    if unmatched.is_empty() {
        return Ok(());
    }
    let ids = unmatched
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| value.to_string())
        })
        .collect::<Vec<_>>()
        .join(", ");
    bail!("roots apply preflight rejected unmatched runtime grants for roots [{ids}]")
}

async fn resolve_api_token(client: &reqwest::Client, args: &ApiAuthArgs) -> anyhow::Result<String> {
    if let Some(token) = &args.token {
        return Ok(token.clone());
    }
    resolve_api_login_token(client, args).await
}

async fn resolve_api_login_token(
    client: &reqwest::Client,
    args: &ApiAuthArgs,
) -> anyhow::Result<String> {
    let username = args
        .username
        .as_deref()
        .context("missing --token or --username")?;
    let password = resolve_api_password(args, username)?;
    login_api(client, &args.api_base, username, &password).await
}

fn resolve_api_password(args: &ApiAuthArgs, username: &str) -> anyhow::Result<String> {
    if args.password.is_some() && args.password_file.is_some() {
        bail!("pass only one of --password or --password-file");
    }
    if let Some(password) = &args.password {
        return Ok(password.clone());
    }
    let path = args
        .password_file
        .as_deref()
        .context("missing --token, --password, or --password-file")?;
    load_password_file(path, username)
}

fn load_password_file(path: &Path, username: &str) -> anyhow::Result<String> {
    let content =
        fs::read_to_string(path).with_context(|| format!("read {} failed", path.display()))?;
    if let Some(secret) = parse_shadow_password(&content, username)? {
        return Ok(secret);
    }
    let password = content.trim();
    if password.is_empty() {
        bail!("password file {} is empty", path.display());
    }
    Ok(password.to_string())
}

fn parse_shadow_password(content: &str, username: &str) -> anyhow::Result<Option<String>> {
    for (index, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields = line.split(':').collect::<Vec<_>>();
        if fields.len() < 2 || fields[0] != username {
            continue;
        }
        let password_hash = fields[1];
        if let Some(password) = password_hash.strip_prefix("plain$") {
            if password.is_empty() {
                bail!(
                    "shadow file line {} for user {username} has empty plain password",
                    index + 1
                );
            }
            return Ok(Some(password.to_string()));
        }
        bail!(
            "password file looks like a shadow file but user {username} is not stored with a recoverable plain$ password"
        );
    }
    Ok(None)
}

async fn login_api(
    client: &reqwest::Client,
    api_base: &str,
    username: &str,
    password: &str,
) -> anyhow::Result<String> {
    let resp = client
        .post(format!("{api_base}/api/fs-meta/v1/session/login"))
        .json(&json!({ "username": username, "password": password }))
        .send()
        .await?;
    if resp.status() != StatusCode::OK {
        let body = resp.text().await.unwrap_or_default();
        bail!("login failed: {body}");
    }
    let payload = resp.json::<SessionLoginResponse>().await?;
    Ok(payload.token)
}

async fn get_json(
    client: &reqwest::Client,
    api_base: &str,
    path: &str,
    token: &str,
) -> anyhow::Result<serde_json::Value> {
    let resp = client
        .get(format!("{api_base}{path}"))
        .bearer_auth(token)
        .send()
        .await?;
    decode_api_response(resp).await
}

async fn post_json(
    client: &reqwest::Client,
    api_base: &str,
    path: &str,
    token: &str,
    payload: &serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    let resp = client
        .post(format!("{api_base}{path}"))
        .bearer_auth(token)
        .json(payload)
        .send()
        .await?;
    decode_api_response(resp).await
}

async fn put_json(
    client: &reqwest::Client,
    api_base: &str,
    path: &str,
    token: &str,
    payload: &serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    let resp = client
        .put(format!("{api_base}{path}"))
        .bearer_auth(token)
        .json(payload)
        .send()
        .await?;
    decode_api_response(resp).await
}

async fn decode_api_response(resp: reqwest::Response) -> anyhow::Result<serde_json::Value> {
    let status = resp.status();
    let body = resp.text().await?;
    let parsed = serde_json::from_str::<serde_json::Value>(&body)
        .unwrap_or_else(|_| json!({ "body": body }));
    if !status.is_success() {
        bail!("api request failed ({}): {}", status, parsed);
    }
    Ok(parsed)
}

fn ensure_fixture_binary() -> anyhow::Result<PathBuf> {
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_fs_meta_api_fixture") {
        let p = PathBuf::from(path);
        if p.exists() {
            return Ok(p);
        }
    }
    let root = workspace_root()?;
    let bin = root.join("target/debug/fs_meta_api_fixture");
    if bin.exists() {
        return Ok(bin);
    }
    let cargo_bin = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let output = Command::new(cargo_bin)
        .current_dir(&root)
        .arg("build")
        .arg("-p")
        .arg("fs-meta-runtime")
        .arg("--bin")
        .arg("fs_meta_api_fixture")
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if !stderr.is_empty() { stderr } else { stdout };
        bail!("cargo build for fs_meta_api_fixture failed: {detail}");
    }
    Ok(bin)
}

async fn wait_for_local_ready(
    api_base_url: &str,
    username: &str,
    password: &str,
    child: &mut Child,
) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(status) = child.try_wait()? {
            bail!("local runtime exited early: {status}");
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("timed out waiting for local runtime");
        }
        if login_api(&client, api_base_url, username, password)
            .await
            .is_ok()
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn pid_is_running(pid: u32) -> bool {
    PathBuf::from(format!("/proc/{pid}")).exists()
}

fn load_local_state(path: &Path) -> anyhow::Result<LocalRuntimeState> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("read local runtime state {} failed", path.display()))?;
    Ok(serde_json::from_str(&content)?)
}

fn prepare_state_dir(base: Option<&Path>, leaf: &str) -> anyhow::Result<PathBuf> {
    let root = base
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let dir = root.join(leaf);
    fs::create_dir_all(&dir)?;
    dir.canonicalize()
        .with_context(|| format!("canonicalize state dir {} failed", dir.display()))
}

fn resolve_fs_meta_runtime_inputs() -> anyhow::Result<(PathBuf, PathBuf)> {
    let root = workspace_root()?;
    let manifest_path = root
        .join("fs-meta/fixtures/manifests/fs-meta.yaml")
        .canonicalize()
        .context("resolve fs-meta manifest path failed")?;
    let binary_path = ensure_fs_meta_app_runtime_path()?;
    Ok((binary_path, manifest_path))
}

fn build_deploy_intent(
    product: &ProductConfig,
    auth: ApiAuthConfig,
    app_target: &Path,
    base_manifest_path: &Path,
    generated_manifest_dir: &Path,
    route_plan_node_ids: Vec<String>,
) -> anyhow::Result<serde_json::Value> {
    build_deploy_intent_for_roots(
        product,
        auth,
        app_target,
        base_manifest_path,
        generated_manifest_dir,
        route_plan_node_ids,
        Vec::new(),
    )
}

fn build_deploy_intent_for_roots(
    product: &ProductConfig,
    auth: ApiAuthConfig,
    app_target: &Path,
    base_manifest_path: &Path,
    generated_manifest_dir: &Path,
    route_plan_node_ids: Vec<String>,
    roots: Vec<RootEntry>,
) -> anyhow::Result<serde_json::Value> {
    let worker_modes = resolve_release_worker_modes(product)?;
    let spec = FsMetaReleaseSpec {
        app_id: DEFAULT_APP_ID.to_string(),
        api_facade_resource_id: product.api.facade_resource_id.clone(),
        auth,
        roots,
        route_plan_node_ids,
        worker_module_path: Some(app_target.to_path_buf()),
        worker_modes,
    };
    let generated_manifest_path =
        generated_manifest_dir.join(format!("fs-meta-startup-{}.yaml", now_us()));
    write_startup_manifest(base_manifest_path, &spec, &generated_manifest_path)
        .map_err(|err| anyhow::anyhow!("generate fs-meta startup manifest failed: {err}"))?;
    let mut release_doc = build_release_doc_value(&spec);
    apply_source_tuning(product, &mut release_doc)?;
    let target_generation = release_doc
        .get("target_generation")
        .and_then(serde_json::Value::as_u64)
        .context("release document missing target_generation")?;
    let unit = release_doc
        .get_mut("units")
        .and_then(serde_json::Value::as_array_mut)
        .and_then(|units| units.first_mut())
        .context("release document missing first unit")?;
    let startup = unit
        .get_mut("startup")
        .and_then(serde_json::Value::as_object_mut)
        .context("release document missing startup section")?;
    startup.insert("path".into(), json!(app_target.display().to_string()));
    startup.insert(
        "manifest".into(),
        json!(generated_manifest_path.display().to_string()),
    );
    let policy = unit
        .get_mut("policy")
        .and_then(serde_json::Value::as_object_mut)
        .context("release document missing policy section")?;
    policy.insert("generation".into(), json!(target_generation));
    compile_release_doc_to_relation_target_intent(&release_doc)
        .map_err(|err| anyhow::anyhow!("invalid fs-meta deploy intent: {err}"))
}

fn apply_source_tuning(
    product: &ProductConfig,
    release_doc: &mut serde_json::Value,
) -> anyhow::Result<()> {
    let Some(config) = release_doc
        .get_mut("units")
        .and_then(serde_json::Value::as_array_mut)
        .and_then(|units| units.first_mut())
        .and_then(|unit| unit.get_mut("config"))
        .and_then(serde_json::Value::as_object_mut)
    else {
        bail!("release document missing first unit config");
    };

    let mut set_int = |key: &str, value: Option<u64>| -> anyhow::Result<()> {
        let Some(value) = value else {
            return Ok(());
        };
        let value = i64::try_from(value)
            .with_context(|| format!("{key} exceeds fs-meta manifest integer range"))?;
        config.insert(key.to_string(), json!(value));
        Ok(())
    };

    set_int("scan_workers", product.scan_workers)?;
    set_int("max_scan_events", product.max_scan_events)?;
    set_int("audit_interval_ms", product.audit_interval_ms)?;
    set_int("throttle_interval_ms", product.throttle_interval_ms)?;
    Ok(())
}

fn ensure_fs_meta_app_runtime_path() -> anyhow::Result<PathBuf> {
    if let Some(path) = [
        std::env::var("CAPANIX_FS_META_APP_BINARY").ok(),
        std::env::var("DATANIX_FS_META_APP_SO").ok(),
    ]
    .into_iter()
    .flatten()
    .map(PathBuf::from)
    .find(|path| path.exists())
    {
        return Ok(path.canonicalize()?);
    }
    let root = workspace_root()?;
    let bin = root.join(format!("target/debug/{}", fs_meta_runtime_library_name()));
    if bin.exists() {
        return Ok(bin.canonicalize()?);
    }
    let cargo_bin = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let output = Command::new(cargo_bin)
        .current_dir(&root)
        .arg("build")
        .arg("-p")
        .arg("fs-meta-runtime")
        .arg("--lib")
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if !stderr.is_empty() { stderr } else { stdout };
        bail!("cargo build for fs-meta-runtime failed: {detail}");
    }
    Ok(bin.canonicalize()?)
}

fn fs_meta_runtime_library_name() -> &'static str {
    #[cfg(target_os = "macos")]
    {
        return "libfs_meta_runtime.dylib";
    }
    #[cfg(target_os = "windows")]
    {
        return "fs_meta_runtime.dll";
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        "libfs_meta_runtime.so"
    }
}

fn workspace_root() -> anyhow::Result<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    Ok(manifest_dir.join("../..").canonicalize()?)
}

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

struct ControlClient {
    socket_path: String,
    actor_id: String,
    domain_id: String,
    key_id: String,
    admin_sk_b64: String,
}

impl ControlClient {
    fn from_args(args: &ControlAuthArgs) -> anyhow::Result<Self> {
        let admin_sk_b64 = args
            .admin_sk_b64
            .clone()
            .or_else(|| std::env::var("DATANIX_CTL_SK_B64").ok())
            .context("missing --admin-sk-b64 or CAPANIX_CTL_SK_B64")?;
        Ok(Self {
            socket_path: resolve_socket_path(args.socket.as_deref()),
            actor_id: args
                .actor_id
                .clone()
                .or_else(|| std::env::var("USER").ok())
                .unwrap_or_else(|| "unknown".to_string()),
            domain_id: args.domain_id.clone(),
            key_id: args.key_id.clone(),
            admin_sk_b64,
        })
    }

    async fn apply_relation_target_intent(
        &self,
        intent: &serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        let path = write_runtime_admin_temp_file("fsmeta-intent", intent)?;
        let output = self
            .run_cnxctl(&[
                "app",
                "apply",
                path.to_str().context("non-utf8 intent path")?,
            ])
            .await;
        let _ = fs::remove_file(&path);
        output
    }

    async fn clear_relation_target(&self, target_id: String) -> anyhow::Result<serde_json::Value> {
        self.run_cnxctl(&["config", "relation-target-clear", &target_id])
            .await
    }

    async fn discover_route_plan_node_ids(&self) -> anyhow::Result<Vec<String>> {
        let config_dump = self.run_cnxctl(&["config", "dump"]).await?;
        let node_ids = derive_route_plan_node_ids_from_config_dump(&config_dump);
        if node_ids.is_empty() {
            bail!(
                "route plan node discovery returned no nodes from control socket {}",
                self.socket_path
            );
        }
        Ok(node_ids)
    }

    async fn run_cnxctl(&self, args: &[&str]) -> anyhow::Result<serde_json::Value> {
        let bin = resolve_cnxctl_bin();
        let output = Command::new(&bin)
            .arg("--output")
            .arg("json")
            .arg("--socket")
            .arg(&self.socket_path)
            .arg("--actor-id")
            .arg(&self.actor_id)
            .arg("--domain-id")
            .arg(&self.domain_id)
            .arg("--key-id")
            .arg(&self.key_id)
            .arg("--admin-sk-b64")
            .arg(&self.admin_sk_b64)
            .args(args)
            .output()
            .with_context(|| format!("spawn {} failed", bin.display()))?;
        if output.status.success() {
            let parsed: serde_json::Value = serde_json::from_slice(&output.stdout)
                .context("parse cnxctl stdout json failed")?;
            return Ok(parsed
                .get("result")
                .cloned()
                .unwrap_or(serde_json::Value::Null));
        }
        let stderr = if output.stderr.is_empty() {
            String::from_utf8_lossy(&output.stdout).into_owned()
        } else {
            String::from_utf8_lossy(&output.stderr).into_owned()
        };
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&stderr) {
            if let Some(message) = parsed
                .get("error")
                .and_then(|value| value.get("message"))
                .and_then(serde_json::Value::as_str)
            {
                bail!(message.to_string());
            }
        }
        bail!(
            "{} failed with status {}: {}",
            bin.display(),
            output.status,
            stderr.trim()
        );
    }
}

fn resolve_socket_path(cli_path: Option<&str>) -> String {
    if let Some(path) = cli_path {
        return path.to_string();
    }
    if let Ok(home) = std::env::var("CAPANIX_HOME") {
        return format!("{home}/core.sock");
    }
    if let Ok(home) = std::env::var("DATANIX_HOME") {
        return format!("{home}/core.sock");
    }
    "core.sock".to_string()
}

fn resolve_cnxctl_bin() -> PathBuf {
    if let Ok(path) = std::env::var("CNXCTL_BIN") {
        let path = PathBuf::from(path);
        if path.exists() {
            return path;
        }
    }
    PathBuf::from("cnxctl")
}

fn derive_route_plan_node_ids_from_config_dump(config_dump: &serde_json::Value) -> Vec<String> {
    let mut node_ids = std::collections::BTreeSet::new();

    if let Some(node_id) = config_dump
        .get("node_id")
        .and_then(serde_json::Value::as_str)
    {
        if !node_id.is_empty() {
            node_ids.insert(node_id.to_string());
        }
    }

    if let Some(resources) = config_dump
        .get("announced_resources")
        .and_then(serde_json::Value::as_array)
    {
        for resource in resources {
            if let Some(node_id) = resource.get("node_id").and_then(serde_json::Value::as_str) {
                if !node_id.is_empty() {
                    node_ids.insert(node_id.to_string());
                }
            }
        }
    }

    if let Some(peers) = config_dump
        .get("management_peers")
        .and_then(serde_json::Value::as_array)
    {
        for peer in peers {
            if let Some(node_id) = peer.get("node_id").and_then(serde_json::Value::as_str) {
                if !node_id.is_empty() {
                    node_ids.insert(node_id.to_string());
                }
            }
        }
    }

    node_ids.into_iter().collect()
}

fn write_runtime_admin_temp_file(
    prefix: &str,
    value: &serde_json::Value,
) -> anyhow::Result<PathBuf> {
    let path = std::env::temp_dir().join(format!("{prefix}-{}.yaml", now_us()));
    fs::write(&path, serde_yaml::to_string(value)?)
        .with_context(|| format!("write temp file {} failed", path.display()))?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::{
        ApiAuthArgs, ProductAuthConfig, RootEntry, build_auth_config, build_deploy_intent,
        build_deploy_intent_for_roots, derive_route_plan_node_ids_from_config_dump,
        load_deployed_auth_config, load_password_file, load_product_config, prepare_state_dir,
        resolve_api_password, workspace_root,
    };
    use fs_meta::source::config::RootSelector;
    use serde_json::json;
    use std::fs;
    use std::path::Path;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("fsmeta-cli-{name}-{nanos}.yaml"))
    }

    #[test]
    fn product_config_accepts_thin_bootstrap_surface() {
        let path = unique_temp_path("thin-config");
        fs::write(
            &path,
            "api:\n  facade_resource_id: fs-meta-tcp-listener\nauth:\n  bootstrap_management:\n    username: admin\n",
        )
        .expect("write thin config");

        let product = load_product_config(&path).expect("thin product config should parse");
        let _ = fs::remove_file(&path);

        assert_eq!(product.api.facade_resource_id, "fs-meta-tcp-listener");
        assert_eq!(
            product
                .auth
                .bootstrap_management
                .as_ref()
                .expect("bootstrap management")
                .username,
            "admin"
        );
    }

    #[test]
    fn product_config_rejects_business_roots_in_deploy_config() {
        let path = unique_temp_path("roots-in-deploy-config");
        fs::write(
            &path,
            "api:\n  facade_resource_id: fs-meta-tcp-listener\nroots:\n  - id: nfs1\n    selector:\n      host_ip: 10.0.0.11\n      mount_point: /mnt/nfs1\n",
        )
        .expect("write config with roots");

        let err = load_product_config(&path).expect_err("deploy config must reject roots");
        let _ = fs::remove_file(&path);

        assert!(
            format!("{err:?}").contains("unknown field `roots`"),
            "deploy config should fail explicitly instead of silently dropping roots: {err:?}"
        );
    }

    #[test]
    fn build_auth_config_materializes_internal_fields_from_thin_surface() {
        let dir = std::env::temp_dir().join(format!(
            "fsmeta-auth-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        fs::create_dir_all(&dir).expect("create temp dir");

        let (auth_cfg, username, password) = build_auth_config(&ProductAuthConfig::default(), &dir);

        assert_eq!(username, "fsmeta-admin");
        assert!(!password.is_empty());
        assert_eq!(auth_cfg.passwd_path, dir.join("fs-meta.passwd"));
        assert_eq!(auth_cfg.shadow_path, dir.join("fs-meta.shadow"));
        assert_eq!(
            auth_cfg.query_keys_path,
            dir.join("fs-meta.query-keys.json")
        );
        assert_eq!(
            auth_cfg
                .bootstrap_management
                .as_ref()
                .expect("bootstrap management")
                .username,
            username
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn build_auth_config_force_materialization_keeps_deploy_password_current() {
        let dir = std::env::temp_dir().join(format!(
            "fsmeta-auth-force-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let (old_auth_cfg, _, _) = build_auth_config(&ProductAuthConfig::default(), &dir);
        old_auth_cfg
            .materialize_bootstrap_management()
            .expect("materialize old auth");

        let (new_auth_cfg, username, password) =
            build_auth_config(&ProductAuthConfig::default(), &dir);
        new_auth_cfg
            .materialize_bootstrap_management()
            .expect("materialize new auth");

        let shadow = fs::read_to_string(dir.join("fs-meta.shadow")).expect("read shadow");
        assert_eq!(
            shadow,
            format!("{username}:plain${password}:0\n"),
            "deploy must rewrite stale bootstrap login files so the returned password is immediately usable",
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn password_file_accepts_fsmeta_plain_shadow_format() {
        let path = unique_temp_path("shadow-password");
        fs::write(
            &path,
            "admin:plain$secret-from-shadow:0\nother:plain$wrong:0\n",
        )
        .expect("write shadow file");

        let password = load_password_file(&path, "admin").expect("plain shadow password");

        assert_eq!(password, "secret-from-shadow");
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn password_file_accepts_raw_password_file() {
        let path = unique_temp_path("raw-password");
        fs::write(&path, "raw-secret\n").expect("write password file");

        let password = load_password_file(&path, "admin").expect("raw password");

        assert_eq!(password, "raw-secret");
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn api_password_rejects_inline_and_file_together() {
        let args = ApiAuthArgs {
            api_base: "http://127.0.0.1:18080".to_string(),
            token: None,
            username: Some("admin".to_string()),
            password: Some("inline".to_string()),
            password_file: Some(PathBuf::from("/tmp/secret")),
        };

        let err =
            resolve_api_password(&args, "admin").expect_err("conflicting password inputs fail");

        assert!(
            format!("{err:#}").contains("pass only one of --password or --password-file"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn deploy_intent_uses_scope_worker_schema_and_shared_config_validation() {
        let path = unique_temp_path("deploy-intent-config");
        fs::write(
            &path,
            "api:\n  facade_resource_id: fs-meta-tcp-listener\nauth:\n  bootstrap_management:\n    username: admin\n",
        )
        .expect("write thin config");
        let product = load_product_config(&path).expect("thin product config should parse");
        let _ = fs::remove_file(&path);

        let dir = std::env::temp_dir().join(format!(
            "fsmeta-intent-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let (auth_cfg, ..) = build_auth_config(&ProductAuthConfig::default(), &dir);

        let manifest = workspace_root()
            .expect("workspace root")
            .join("fs-meta/fixtures/manifests/fs-meta.yaml")
            .canonicalize()
            .expect("fixture manifest path");
        let app_target = PathBuf::from("/tmp/fs-meta-runtime.so");
        let intent = build_deploy_intent(
            &product,
            auth_cfg,
            &app_target,
            &manifest,
            &dir,
            vec!["node-a".to_string(), "node-b".to_string()],
        )
        .expect("deploy intent should compile through shared config boundary");

        assert_eq!(
            intent["schema_version"],
            json!("scope-worker-declaration-v1")
        );
        assert_eq!(intent["target_id"], json!("fs-meta"));
        let workers_array = intent["workers"]
            .as_array()
            .expect("worker intent should encode workers");
        assert_eq!(workers_array.len(), 1);
        let worker = &workers_array[0];
        assert_eq!(worker["worker_role"], json!("main"));
        assert_eq!(worker["startup"]["path"], json!("/tmp/fs-meta-runtime.so"));
        let generated_manifest = worker["startup"]["manifest"]
            .as_str()
            .expect("compiled startup manifest path");
        assert!(
            generated_manifest.starts_with(dir.to_string_lossy().as_ref()),
            "generated startup manifest should live under the deploy state dir: {generated_manifest}"
        );
        assert!(
            PathBuf::from(generated_manifest).exists(),
            "generated startup manifest must be materialized on disk"
        );
        let workers = worker["config"]["workers"]
            .as_object()
            .expect("compiled fs-meta release config should declare workers");
        assert_eq!(
            workers
                .get("facade")
                .and_then(|row| row.get("mode"))
                .and_then(serde_json::Value::as_str),
            Some("embedded")
        );
        for role in ["source", "sink"] {
            let row = workers
                .get(role)
                .expect("external worker config should exist")
                .as_object()
                .expect("worker config should encode as an object");
            assert_eq!(
                row.get("mode").and_then(serde_json::Value::as_str),
                Some("external")
            );
            let module_path = row
                .get("startup")
                .and_then(serde_json::Value::as_object)
                .and_then(|startup| startup.get("path"));
            assert_eq!(
                module_path.and_then(serde_json::Value::as_str),
                Some("/tmp/fs-meta-runtime.so")
            );
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn roots_apply_deploy_intent_carries_business_root_app_scopes() {
        let path = unique_temp_path("roots-apply-intent-config");
        fs::write(&path, "api:\n  facade_resource_id: fs-meta-tcp-listener\n")
            .expect("write product config");
        let product = load_product_config(&path).expect("product config should parse");
        let _ = fs::remove_file(&path);

        let dir = std::env::temp_dir().join(format!(
            "fsmeta-roots-apply-intent-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let (auth_cfg, ..) = build_auth_config(&ProductAuthConfig::default(), &dir);

        let manifest = workspace_root()
            .expect("workspace root")
            .join("fs-meta/fixtures/manifests/fs-meta.yaml")
            .canonicalize()
            .expect("fixture manifest path");
        let roots = vec![
            RootEntry {
                id: "nfs-144".to_string(),
                selector: RootSelector {
                    host_ref: Some("panda144".to_string()),
                    ..RootSelector::default()
                },
                subpath_scope: PathBuf::from("/"),
                watch: true,
                scan: true,
                audit_interval_ms: None,
            },
            RootEntry {
                id: "nfs-145".to_string(),
                selector: RootSelector {
                    host_ref: Some("panda145".to_string()),
                    ..RootSelector::default()
                },
                subpath_scope: PathBuf::from("/"),
                watch: true,
                scan: true,
                audit_interval_ms: None,
            },
        ];
        let intent = build_deploy_intent_for_roots(
            &product,
            auth_cfg,
            &PathBuf::from("/tmp/fs-meta-runtime.so"),
            &manifest,
            &dir,
            vec!["panda144".to_string(), "panda145".to_string()],
            roots,
        )
        .expect("roots-bearing intent should compile");

        let app_scopes = intent["workers"][0]["runtime"]["app_scopes"]
            .as_array()
            .expect("compiled intent should carry runtime app_scopes");
        let scope_ids = app_scopes
            .iter()
            .filter_map(|scope| scope.get("scope_id").and_then(serde_json::Value::as_str))
            .collect::<std::collections::BTreeSet<_>>();
        assert!(scope_ids.contains("nfs-144"));
        assert!(scope_ids.contains("nfs-145"));
        assert!(scope_ids.contains("fs-meta-tcp-listener"));
        assert!(
            !scope_ids.contains("__fsmeta_empty_roots_bootstrap"),
            "roots-bearing relation target must replace bootstrap-only app scopes"
        );
        let nfs_144_scope = app_scopes
            .iter()
            .find(|scope| scope.get("scope_id") == Some(&json!("nfs-144")))
            .expect("nfs-144 app scope");
        let unit_scopes = nfs_144_scope["unit_scopes"]
            .as_array()
            .expect("nfs root unit scopes");
        assert!(unit_scopes.iter().any(|unit| {
            unit.get("unit_id") == Some(&json!("runtime.exec.sink"))
                && unit.get("eligibility") == Some(&json!("resource_visible_nodes"))
                && unit.get("cardinality") == Some(&json!("one"))
        }));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn roots_apply_config_requires_existing_deployed_auth_files() {
        let dir = std::env::temp_dir().join(format!(
            "fsmeta-missing-auth-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        fs::create_dir_all(&dir).expect("create temp dir");

        let err =
            load_deployed_auth_config(&dir).expect_err("missing deployed auth files must fail");

        assert!(
            format!("{err:#}").contains("deployed fs-meta auth files are missing"),
            "unexpected error: {err:#}"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn prepare_state_dir_returns_absolute_path_for_relative_deploy_config() {
        let cwd = std::env::current_dir().expect("current dir");
        let relative = Path::new(".");
        let state_dir =
            prepare_state_dir(Some(relative), ".fsmeta-state").expect("prepare state dir");

        assert!(
            state_dir.is_absolute(),
            "deploy-generated manifest paths must be absolute so daemon-side runtime discovery does not depend on the fsmeta CLI working directory; state_dir={}",
            state_dir.display()
        );

        let expected = cwd
            .join(".fsmeta-state")
            .canonicalize()
            .expect("canonical state dir");
        assert_eq!(state_dir, expected);
    }

    #[test]
    fn deploy_intent_allows_generation_based_source_mode_reconfiguration() {
        let path = unique_temp_path("deploy-intent-source-embedded");
        fs::write(
            &path,
            "api:\n  facade_resource_id: fs-meta-tcp-listener\nauth:\n  bootstrap_management:\n    username: admin\nworkers:\n  source:\n    mode: embedded\n",
        )
        .expect("write product config");
        let product = load_product_config(&path).expect("product config should parse");
        let _ = fs::remove_file(&path);

        let dir = std::env::temp_dir().join(format!(
            "fsmeta-intent-source-embedded-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let (auth_cfg, ..) = build_auth_config(&ProductAuthConfig::default(), &dir);

        let manifest = workspace_root()
            .expect("workspace root")
            .join("fs-meta/fixtures/manifests/fs-meta.yaml")
            .canonicalize()
            .expect("fixture manifest path");
        let app_target = PathBuf::from("/tmp/fs-meta-runtime.so");
        let intent = build_deploy_intent(
            &product,
            auth_cfg,
            &app_target,
            &manifest,
            &dir,
            vec!["node-a".to_string(), "node-b".to_string()],
        )
        .expect("deploy intent should compile through shared config boundary");
        let workers = intent["workers"][0]["config"]["workers"]
            .as_object()
            .expect("compiled fs-meta release config should declare workers");
        for role in ["source"] {
            let row = workers
                .get(role)
                .expect("source worker config should exist")
                .as_object()
                .expect("worker config should encode as an object");
            assert_eq!(
                row.get("mode").and_then(serde_json::Value::as_str),
                Some("embedded")
            );
            assert!(
                row.get("startup").is_none(),
                "embedded source mode must not emit startup.path"
            );
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn deploy_intent_carries_source_tuning_fields_into_app_config() {
        let path = unique_temp_path("deploy-intent-source-tuning");
        fs::write(
            &path,
            "api:\n  facade_resource_id: fs-meta-tcp-listener\nscan_workers: 16\nmax_scan_events: 10000000\naudit_interval_ms: 300000\nthrottle_interval_ms: 50\n",
        )
        .expect("write product config");
        let product = load_product_config(&path).expect("product config should parse");
        let _ = fs::remove_file(&path);

        let dir = std::env::temp_dir().join(format!(
            "fsmeta-intent-source-tuning-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let (auth_cfg, ..) = build_auth_config(&ProductAuthConfig::default(), &dir);

        let manifest = workspace_root()
            .expect("workspace root")
            .join("fs-meta/fixtures/manifests/fs-meta.yaml")
            .canonicalize()
            .expect("fixture manifest path");
        let intent = build_deploy_intent(
            &product,
            auth_cfg,
            &PathBuf::from("/tmp/fs-meta-runtime.so"),
            &manifest,
            &dir,
            vec!["node-a".to_string()],
        )
        .expect("deploy intent should compile");
        let config = intent["workers"][0]["config"]
            .as_object()
            .expect("compiled app config should be a map");

        assert_eq!(config.get("scan_workers"), Some(&json!(16)));
        assert_eq!(config.get("max_scan_events"), Some(&json!(10_000_000)));
        assert_eq!(config.get("audit_interval_ms"), Some(&json!(300_000)));
        assert_eq!(config.get("throttle_interval_ms"), Some(&json!(50)));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn product_config_rejects_removed_fourth_worker_surface() {
        let path = unique_temp_path("reject-legacy-scan-role");
        fs::write(
            &path,
            "api:\n  facade_resource_id: fs-meta-tcp-listener\nworkers:\n  scan:\n    mode: external\n",
        )
        .expect("write invalid config");

        let err = load_product_config(&path).expect_err("workers.scan must be rejected");
        let _ = fs::remove_file(&path);
        let err_text = format!("{err:#}");

        assert!(
            err_text.contains("parse config") && err_text.contains("unknown field `scan`"),
            "unexpected parse error: {err_text}"
        );
    }

    #[test]
    fn route_plan_node_discovery_collects_unique_nodes_from_config_dump() {
        let dump = json!({
            "node_id": "panda145",
            "announced_resources": [
                {"node_id": "panda144"},
                {"node_id": "panda145"},
                {"node_id": "panda147"}
            ],
            "management_peers": [
                {"node_id": "panda146"},
                {"node_id": "panda147"},
                {"node_id": ""}
            ]
        });

        let actual = derive_route_plan_node_ids_from_config_dump(&dump);

        assert_eq!(
            actual,
            vec![
                "panda144".to_string(),
                "panda145".to_string(),
                "panda146".to_string(),
                "panda147".to_string(),
            ]
        );
    }
}
