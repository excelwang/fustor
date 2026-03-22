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
    compile_release_doc_to_relation_target_intent,
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
    Preview(RootsCommandArgs),
    Apply(RootsCommandArgs),
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
}

#[derive(Args, Debug)]
struct RootsCommandArgs {
    #[command(flatten)]
    api: ApiAuthArgs,
    #[arg(long)]
    file: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionLoginResponse {
    token: String,
    expires_in_secs: u64,
    user: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct ProductConfig {
    api: ProductApiConfig,
    #[serde(default)]
    auth: ProductAuthConfig,
    #[serde(default)]
    workers: ProductWorkersConfig,
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
    let (app_target, manifest_path) = resolve_fs_meta_runtime_inputs()?;
    let intent = build_deploy_intent(&product, auth_cfg, &app_target, &manifest_path)?;
    let control = ControlClient::from_args(&args.control)?;
    let result = control.apply_relation_target_intent(&intent).await?;
    Ok(json!({
        "status": "ok",
        "app_id": DEFAULT_APP_ID,
        "api_facade_resource_id": product.api.facade_resource_id,
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
    fs::write(
        &auth_cfg.passwd_path,
        format!(
            "{}:{}:{}:{}:/home/{}:/bin/bash:0\n",
            username, 1000, 1000, auth_cfg.management_group, username,
        ),
    )?;
    fs::write(
        &auth_cfg.shadow_path,
        format!("{}:plain${}:0\n", username, password),
    )?;

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

async fn roots_preview(args: RootsCommandArgs) -> anyhow::Result<serde_json::Value> {
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

async fn roots_apply(args: RootsCommandArgs) -> anyhow::Result<serde_json::Value> {
    let client = reqwest::Client::new();
    let token = resolve_api_token(&client, &args.api).await?;
    let payload = load_roots_payload(&args.file)?;
    put_json(
        &client,
        &args.api.api_base,
        "/api/fs-meta/v1/monitoring/roots",
        &token,
        &payload,
    )
    .await
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

async fn resolve_api_token(client: &reqwest::Client, args: &ApiAuthArgs) -> anyhow::Result<String> {
    if let Some(token) = &args.token {
        return Ok(token.clone());
    }
    let username = args
        .username
        .as_deref()
        .context("missing --token or --username")?;
    let password = args
        .password
        .as_deref()
        .context("missing --token or --password")?;
    login_api(client, &args.api_base, username, password).await
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
    let status = Command::new(cargo_bin)
        .current_dir(&root)
        .arg("build")
        .arg("-p")
        .arg("fs-meta-runtime")
        .arg("--bin")
        .arg("fs_meta_api_fixture")
        .status()?;
    if !status.success() {
        bail!("cargo build for fs_meta_api_fixture failed");
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
    Ok(dir)
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
    manifest_path: &Path,
) -> anyhow::Result<serde_json::Value> {
    let worker_modes = resolve_release_worker_modes(product)?;
    let mut release_doc = build_release_doc_value(&FsMetaReleaseSpec {
        app_id: DEFAULT_APP_ID.to_string(),
        api_facade_resource_id: product.api.facade_resource_id.clone(),
        auth,
        roots: Vec::new(),
        worker_module_path: Some(app_target.to_path_buf()),
        worker_modes,
    });
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
        json!(manifest_path.display().to_string()),
    );
    let policy = unit
        .get_mut("policy")
        .and_then(serde_json::Value::as_object_mut)
        .context("release document missing policy section")?;
    policy.insert("generation".into(), json!(target_generation));
    compile_release_doc_to_relation_target_intent(&release_doc)
        .map_err(|err| anyhow::anyhow!("invalid fs-meta deploy intent: {err}"))
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
    let status = Command::new(cargo_bin)
        .current_dir(&root)
        .arg("build")
        .arg("-p")
        .arg("fs-meta-runtime")
        .arg("--lib")
        .status()?;
    if !status.success() {
        bail!("cargo build for fs-meta-runtime failed");
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
        ProductAuthConfig, build_auth_config, build_deploy_intent, load_product_config,
        workspace_root,
    };
    use serde_json::json;
    use std::fs;
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
        let intent = build_deploy_intent(&product, auth_cfg, &app_target, &manifest)
            .expect("deploy intent should compile through shared config boundary");

        assert_eq!(intent["schema_version"], json!("scope-worker-intent-v1"));
        assert_eq!(intent["target_id"], json!("fs-meta"));
        let workers_array = intent["workers"]
            .as_array()
            .expect("worker intent should encode workers");
        assert_eq!(workers_array.len(), 1);
        let worker = &workers_array[0];
        assert_eq!(worker["worker_role"], json!("main"));
        assert_eq!(worker["startup"]["path"], json!("/tmp/fs-meta-runtime.so"));
        assert_eq!(
            worker["startup"]["manifest"],
            json!(manifest.to_string_lossy().as_ref())
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
        let intent = build_deploy_intent(&product, auth_cfg, &app_target, &manifest)
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
}
