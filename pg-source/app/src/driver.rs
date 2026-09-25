use std::str::FromStr;
use std::time::Duration;

use capanix_app_sdk::{CnxError, Result};
use native_tls::TlsConnector;
use pg_source::shared_types::{PgCatalogMetadata, PgFieldCatalog, PgFieldDescriptor, PgProbeStatus, PgRowEvent};
use pg_source::{ResolvedCredential, ResolvedPgEndpoint};
use postgres::config::SslMode;
use postgres::{Client, Config, NoTls};
use postgres_native_tls::MakeTlsConnector;
use serde_json::{Map, Number, Value};
use source_kit::SourceOperationBudget;
use url::Url;

pub trait PgSourceDriver: Send + Sync {
    fn catalog_metadata_with_budget(
        &self,
        _endpoint: &ResolvedPgEndpoint,
        _credential: &ResolvedCredential,
        _limit: usize,
        _budget: &mut SourceOperationBudget,
    ) -> Result<PgCatalogMetadata> {
        Err(CnxError::NotSupported("PostgreSQL catalog metadata is not supported by this driver".into()))
    }

    fn probe(
        &self,
        endpoint: &ResolvedPgEndpoint,
        credential: &ResolvedCredential,
    ) -> Result<PgProbeStatus>;

    fn probe_with_timeout(
        &self,
        endpoint: &ResolvedPgEndpoint,
        credential: &ResolvedCredential,
        timeout: Option<Duration>,
    ) -> Result<PgProbeStatus> {
        let _ = timeout;
        self.probe(endpoint, credential)
    }

    fn probe_with_budget(
        &self,
        endpoint: &ResolvedPgEndpoint,
        credential: &ResolvedCredential,
        budget: &mut SourceOperationBudget,
    ) -> Result<PgProbeStatus> {
        budget.observe_fanout(1)?;
        budget.consume_call(1)?;
        self.probe_with_timeout(endpoint, credential, budget.remaining_timeout()?)
    }

    fn discover_fields(
        &self,
        endpoint: &ResolvedPgEndpoint,
        credential: &ResolvedCredential,
    ) -> Result<PgFieldCatalog>;

    fn discover_fields_with_timeout(
        &self,
        endpoint: &ResolvedPgEndpoint,
        credential: &ResolvedCredential,
        timeout: Option<Duration>,
    ) -> Result<PgFieldCatalog> {
        let _ = timeout;
        self.discover_fields(endpoint, credential)
    }

    fn discover_fields_with_budget(
        &self,
        endpoint: &ResolvedPgEndpoint,
        credential: &ResolvedCredential,
        budget: &mut SourceOperationBudget,
    ) -> Result<PgFieldCatalog> {
        budget.observe_fanout(1)?;
        budget.consume_call(1)?;
        self.discover_fields_with_timeout(endpoint, credential, budget.remaining_timeout()?)
    }

    fn open_snapshot(
        &self,
        endpoint: &ResolvedPgEndpoint,
        credential: &ResolvedCredential,
        schema: &str,
        table: &str,
        fields: &[String],
    ) -> Result<Box<dyn PgSnapshotSession>>;

    fn open_snapshot_with_timeout(
        &self,
        endpoint: &ResolvedPgEndpoint,
        credential: &ResolvedCredential,
        schema: &str,
        table: &str,
        fields: &[String],
        timeout: Option<Duration>,
    ) -> Result<Box<dyn PgSnapshotSession>> {
        let _ = timeout;
        self.open_snapshot(endpoint, credential, schema, table, fields)
    }

    fn open_snapshot_with_budget(
        &self,
        endpoint: &ResolvedPgEndpoint,
        credential: &ResolvedCredential,
        schema: &str,
        table: &str,
        fields: &[String],
        budget: &mut SourceOperationBudget,
    ) -> Result<Box<dyn PgSnapshotSession>> {
        budget.observe_fanout(1)?;
        budget.consume_call(1)?;
        self.open_snapshot_with_timeout(
            endpoint,
            credential,
            schema,
            table,
            fields,
            budget.remaining_timeout()?,
        )
    }
}

pub trait PgSnapshotSession: Send {
    fn fetch_page(&mut self, limit: usize, last_key: Option<&Value>) -> Result<PgSnapshotPage>;
    fn fetch_page_with_timeout(
        &mut self,
        limit: usize,
        last_key: Option<&Value>,
        timeout: Option<Duration>,
    ) -> Result<PgSnapshotPage> {
        let _ = timeout;
        self.fetch_page(limit, last_key)
    }
    fn fetch_page_with_budget(
        &mut self,
        limit: usize,
        last_key: Option<&Value>,
        budget: &mut SourceOperationBudget,
    ) -> Result<PgSnapshotPage> {
        budget.observe_fanout(1)?;
        budget.consume_call(1)?;
        self.fetch_page_with_timeout(limit, last_key, budget.remaining_timeout()?)
    }
    fn close(&mut self) -> Result<()>;
    fn close_with_budget(&mut self, budget: &mut SourceOperationBudget) -> Result<()> {
        budget.observe_fanout(1)?;
        budget.consume_call(1)?;
        self.close()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PgSnapshotPage {
    pub fields: Vec<String>,
    pub rows: Vec<PgRowEvent>,
    pub source_revision: String,
    pub last_key: Option<Value>,
    pub has_more: bool,
}

#[derive(Default)]
pub struct NativePgSourceDriver;

impl PgSourceDriver for NativePgSourceDriver {
    fn catalog_metadata_with_budget(
        &self,
        endpoint: &ResolvedPgEndpoint,
        credential: &ResolvedCredential,
        limit: usize,
        budget: &mut SourceOperationBudget,
    ) -> Result<PgCatalogMetadata> {
        if limit == 0 || limit > 5000 {
            return Err(CnxError::InvalidInput("catalog metadata limit must be in 1..5000".into()));
        }
        budget.observe_fanout(1)?;
        // Reserve BEGIN/settings, the fixed catalog query, and ROLLBACK before I/O.
        budget.ensure_call_capacity(3, 3)?;
        let timeout = budget.remaining_timeout()?.unwrap_or(Duration::from_secs(10)).min(Duration::from_secs(10));
        run_pg_thread("inspect PostgreSQL catalog", || {
            let started = std::time::Instant::now();
            let mut client = open_client(endpoint, credential, Some(timeout))?;
            let remaining = timeout.checked_sub(started.elapsed()).filter(|t| !t.is_zero()).ok_or(CnxError::Timeout)?;
            let millis = remaining.as_millis().max(1);
            budget.consume_call(1)?;
            let opened = client.simple_query(&format!(
                "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY; SET LOCAL statement_timeout = {millis}; SET LOCAL lock_timeout = {}; SELECT has_function_privilege('pg_catalog.pg_control_system()', 'EXECUTE')",
                millis.min(1000)
            )).map_err(pg_error("begin read-only PostgreSQL catalog observation"))?;
            let can_read_system = opened.iter().find_map(|message| match message {
                postgres::SimpleQueryMessage::Row(row) => row.get(0).map(|value| value == "t"),
                _ => None,
            }).ok_or_else(|| CnxError::ProtocolViolation("missing PostgreSQL catalog privilege observation".into()))?;
            let sql = r#"
WITH visible_objects AS (
  SELECT n.nspname AS schema, c.relname AS name, c.oid::bigint AS oid,
         c.relkind::text AS kind,
         CASE WHEN c.relkind IN ('r','m') AND c.reltuples >= 0
              THEN c.reltuples::bigint ELSE NULL END AS estimated_rows,
         'estimate'::text AS row_count_method,
         CASE WHEN c.relkind IN ('v','m') THEN md5(pg_get_viewdef(c.oid,true))
              ELSE NULL END AS view_definition_md5
  FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
  WHERE c.relkind IN ('r','p','v','m','f')
    AND n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%'
    AND has_schema_privilege(n.oid,'USAGE')
    AND (has_table_privilege(c.oid,'SELECT') OR has_any_column_privilege(c.oid,'SELECT'))
    AND EXISTS (SELECT 1 FROM unnest($1::text[]) AS s(scope) WHERE
      s.scope = n.nspname OR s.scope = '*' OR (strpos(s.scope,'*') > 0 AND
      length(n.nspname) >= length(s.scope)-1 AND
      left(n.nspname,strpos(s.scope,'*')-1)=left(s.scope,strpos(s.scope,'*')-1) AND
      right(n.nspname,length(s.scope)-strpos(s.scope,'*'))=substr(s.scope,strpos(s.scope,'*')+1)))
    AND EXISTS (SELECT 1 FROM unnest($2::text[]) AS s(scope) WHERE
      s.scope = c.relname OR s.scope = '*' OR (strpos(s.scope,'*') > 0 AND
      length(c.relname) >= length(s.scope)-1 AND
      left(c.relname,strpos(s.scope,'*')-1)=left(s.scope,strpos(s.scope,'*')-1) AND
      right(c.relname,length(s.scope)-strpos(s.scope,'*'))=substr(s.scope,strpos(s.scope,'*')+1)))
  ORDER BY n.nspname,c.relname LIMIT $3
)
SELECT json_build_object(
  'object_ref',$4::text,
  'database',current_database(),
  'database_oid',(SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname=current_database()),
  'system_identifier',SYSTEM_IDENTIFIER_EXPRESSION,
  'read_only',current_setting('transaction_read_only')='on',
  'complete',true,
  'grant_epoch',NULL,
  'objects',COALESCE((SELECT json_agg(row_to_json(o) ORDER BY o.schema,o.name) FROM visible_objects o),'[]'::json)
)::text
"#;
            // PostgreSQL checks function EXECUTE while initializing expressions,
            // even in an untaken CASE. Never reference the protected function
            // unless the same native principal positively observed that grant.
            let sql = sql.replace("SYSTEM_IDENTIFIER_EXPRESSION", if can_read_system {
                "(SELECT system_identifier::text FROM pg_catalog.pg_control_system())"
            } else { "NULL" });
            let max_objects = (limit + 1) as i64;
            budget.consume_call(1)?;
            let row = client.query_one(&sql, &[&endpoint.schema_scopes, &endpoint.table_scopes, &max_objects, &endpoint.object_ref])
                .map_err(pg_error("read scoped PostgreSQL catalog"))?;
            let raw: String = row.try_get(0).map_err(pg_error("decode PostgreSQL catalog"))?;
            budget.consume_call(1)?;
            client.batch_execute("ROLLBACK").map_err(pg_error("close PostgreSQL catalog observation"))?;
            let mut catalog: PgCatalogMetadata = serde_json::from_str(&raw)
                .map_err(|_| CnxError::ProtocolViolation("invalid PostgreSQL catalog response".into()))?;
            if !catalog.read_only || catalog.objects.len() > limit {
                return Err(CnxError::ResourceExhausted("PostgreSQL catalog exceeds the declared object bound".into()));
            }
            catalog.grant_epoch = endpoint.grant_epoch;
            Ok(catalog)
        })
    }

    fn probe(
        &self,
        endpoint: &ResolvedPgEndpoint,
        credential: &ResolvedCredential,
    ) -> Result<PgProbeStatus> {
        self.probe_with_timeout(endpoint, credential, None)
    }

    fn probe_with_timeout(
        &self,
        endpoint: &ResolvedPgEndpoint,
        credential: &ResolvedCredential,
        timeout: Option<Duration>,
    ) -> Result<PgProbeStatus> {
        run_pg_thread("probe PostgreSQL endpoint", || {
            let mut client = open_client(endpoint, credential, timeout)?;
            set_statement_timeout(&mut client, timeout, false)?;
            let server_version = client
                .query_one("SELECT version()", &[])
                .map_err(pg_error("probe PostgreSQL endpoint"))?
                .try_get::<_, String>(0)
                .map_err(pg_error("decode PostgreSQL server version"))?;
            Ok(PgProbeStatus {
                object_ref: endpoint.object_ref.clone(),
                endpoint_uri: endpoint.endpoint_uri.clone(),
                reachable: true,
                server_version: Some(server_version),
                diagnostics: None,
            })
        })
    }

    fn discover_fields(
        &self,
        endpoint: &ResolvedPgEndpoint,
        credential: &ResolvedCredential,
    ) -> Result<PgFieldCatalog> {
        self.discover_fields_with_timeout(endpoint, credential, None)
    }

    fn discover_fields_with_timeout(
        &self,
        endpoint: &ResolvedPgEndpoint,
        credential: &ResolvedCredential,
        timeout: Option<Duration>,
    ) -> Result<PgFieldCatalog> {
        run_pg_thread("discover PostgreSQL fields", || {
            let sql = r#"
SELECT table_schema, table_name, column_name, ordinal_position, data_type
FROM information_schema.columns
WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
ORDER BY table_schema, table_name, ordinal_position
"#;
            let mut client = open_client(endpoint, credential, timeout)?;
            set_statement_timeout(&mut client, timeout, false)?;
            let rows = client
                .query(sql, &[])
                .map_err(pg_error("discover PostgreSQL fields"))?;
            let mut fields = Vec::with_capacity(rows.len());
            for row in rows {
                let schema = row
                    .try_get::<_, String>(0)
                    .map_err(pg_error("decode PostgreSQL field schema"))?;
                let table = row
                    .try_get::<_, String>(1)
                    .map_err(pg_error("decode PostgreSQL field table"))?;
                if scope_contains(&schema, &endpoint.schema_scopes)
                    && scope_contains(&table, &endpoint.table_scopes)
                {
                    fields.push(PgFieldDescriptor {
                        schema,
                        table,
                        column: row
                            .try_get::<_, String>(2)
                            .map_err(pg_error("decode PostgreSQL field column"))?,
                        ordinal_position: row
                            .try_get::<_, Option<i32>>(3)
                            .map_err(pg_error("decode PostgreSQL field ordinal"))?
                            .and_then(|value| u64::try_from(value).ok()),
                        column_type: row
                            .try_get::<_, Option<String>>(4)
                            .map_err(pg_error("decode PostgreSQL field type"))?
                            .and_then(nonempty),
                    });
                }
            }
            Ok(PgFieldCatalog {
                object_ref: endpoint.object_ref.clone(),
                fields,
            })
        })
    }

    fn open_snapshot(
        &self,
        endpoint: &ResolvedPgEndpoint,
        credential: &ResolvedCredential,
        schema: &str,
        table: &str,
        fields: &[String],
    ) -> Result<Box<dyn PgSnapshotSession>> {
        self.open_snapshot_with_timeout(endpoint, credential, schema, table, fields, None)
    }

    fn open_snapshot_with_timeout(
        &self,
        endpoint: &ResolvedPgEndpoint,
        credential: &ResolvedCredential,
        schema: &str,
        table: &str,
        fields: &[String],
        timeout: Option<Duration>,
    ) -> Result<Box<dyn PgSnapshotSession>> {
        run_pg_thread("open PostgreSQL snapshot", || {
            let mut client = open_client(endpoint, credential, timeout)?;
            client
                .batch_execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                .map_err(pg_error(
                    "start PostgreSQL repeatable-read snapshot transaction",
                ))?;
            set_statement_timeout(&mut client, timeout, true)?;
            let snapshot_revision = client
                .query_one("SELECT txid_current_snapshot()::text", &[])
                .map_err(pg_error("read PostgreSQL transaction snapshot revision"))?
                .try_get::<_, String>(0)
                .map_err(pg_error("decode PostgreSQL transaction snapshot revision"))?;
            let table_fields = load_table_fields(&mut client, schema, table)?;
            let output_fields = if fields.is_empty() {
                table_fields.clone()
            } else {
                validate_requested_fields(fields, &table_fields)?;
                fields.to_vec()
            };
            if output_fields.is_empty() {
                return Err(CnxError::InvalidInput(format!(
                    "pg table `{schema}`.`{table}` has no selectable fields"
                )));
            }
            let primary_key = match endpoint.primary_key.clone() {
                Some(primary_key) => primary_key,
                None => load_single_primary_key(&mut client, schema, table)?.ok_or_else(|| {
                    CnxError::InvalidInput(format!(
                        "pg snapshot requires a configured or discoverable single-column primary_key for `{schema}`.`{table}`"
                    ))
                })?,
            };
            if !table_fields.iter().any(|field| field == &primary_key) {
                return Err(CnxError::InvalidInput(format!(
                    "pg primary_key '{primary_key}' is not a column of `{schema}`.`{table}`"
                )));
            }
            let key_type = load_key_type(&mut client, schema, table, &primary_key)?;
            let mut select_fields = output_fields.clone();
            if !select_fields.iter().any(|field| field == &primary_key) {
                select_fields.push(primary_key.clone());
            }
            Ok(Box::new(NativePgSnapshotSession {
                client: Some(client),
                schema: schema.to_string(),
                table: table.to_string(),
                output_fields,
                select_fields,
                key_field: primary_key,
                key_type,
                source_revision: format!("pg-txid-snapshot:{snapshot_revision}"),
                closed: false,
            }) as Box<dyn PgSnapshotSession>)
        })
    }
}

struct NativePgSnapshotSession {
    client: Option<Client>,
    schema: String,
    table: String,
    output_fields: Vec<String>,
    select_fields: Vec<String>,
    key_field: String,
    key_type: PgKeyType,
    source_revision: String,
    closed: bool,
}

impl NativePgSnapshotSession {
    fn fetch_page_inner(
        &mut self,
        limit: usize,
        last_key: Option<&Value>,
    ) -> Result<PgSnapshotPage> {
        if self.closed {
            return Err(CnxError::NotReady("pg snapshot session is closed".into()));
        }
        let client = self
            .client
            .as_mut()
            .ok_or_else(|| CnxError::NotReady("pg snapshot session is closed".into()))?;
        let fetch_limit = limit.saturating_add(1);
        let field_list = self
            .select_fields
            .iter()
            .map(|field| quote_identifier(field))
            .collect::<Vec<_>>()
            .join(", ");
        let key_identifier = quote_identifier(&self.key_field);
        let from_clause = format!(
            "{}.{}",
            quote_identifier(&self.schema),
            quote_identifier(&self.table)
        );
        let key_parameter = last_key.map(key_value_to_param_string).transpose()?;
        let where_clause = if key_parameter.is_some() {
            format!(
                "WHERE {key_identifier} > $1::text::{}",
                self.key_type.cast_type
            )
        } else {
            String::new()
        };
        let sql = format!(
            "SELECT row_to_json(fustor_src)::text, fustor_src.{key_identifier}::text \
             FROM (SELECT {field_list} FROM {from_clause} {where_clause} \
             ORDER BY {key_identifier} LIMIT {fetch_limit}) fustor_src"
        );
        let rows = match key_parameter.as_ref() {
            Some(key) => client
                .query(&sql, &[key])
                .map_err(pg_error("fetch PostgreSQL snapshot page"))?,
            None => client
                .query(&sql, &[])
                .map_err(pg_error("fetch PostgreSQL snapshot page"))?,
        };
        let has_more = rows.len() > limit;
        let mut events = Vec::with_capacity(limit.min(rows.len()));
        let mut page_last_key = None;
        for row in rows.into_iter().take(limit) {
            let json_text = row
                .try_get::<_, String>(0)
                .map_err(pg_error("decode PostgreSQL snapshot row"))?;
            let key_text = row
                .try_get::<_, String>(1)
                .map_err(pg_error("decode PostgreSQL snapshot key"))?;
            let (event, key) =
                row_to_event_and_key(&self.output_fields, &self.key_type, &json_text, &key_text)?;
            page_last_key = Some(key);
            events.push(event);
        }
        Ok(PgSnapshotPage {
            fields: self.output_fields.clone(),
            rows: events,
            source_revision: self.source_revision.clone(),
            last_key: page_last_key,
            has_more,
        })
    }

    fn close_inner(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        let Some(mut client) = self.client.take() else {
            return Ok(());
        };
        let result = client
            .batch_execute("ROLLBACK")
            .map_err(pg_error("close PostgreSQL snapshot transaction"));
        drop(client);
        result
    }
}

impl PgSnapshotSession for NativePgSnapshotSession {
    fn fetch_page(&mut self, limit: usize, last_key: Option<&Value>) -> Result<PgSnapshotPage> {
        self.fetch_page_with_timeout(limit, last_key, None)
    }

    fn fetch_page_with_timeout(
        &mut self,
        limit: usize,
        last_key: Option<&Value>,
        timeout: Option<Duration>,
    ) -> Result<PgSnapshotPage> {
        run_pg_thread("fetch PostgreSQL snapshot page", || {
            let client = self
                .client
                .as_mut()
                .ok_or_else(|| CnxError::NotReady("pg snapshot session is closed".into()))?;
            set_statement_timeout(client, timeout, true)?;
            self.fetch_page_inner(limit, last_key)
        })
    }

    fn close(&mut self) -> Result<()> {
        run_pg_thread("close PostgreSQL snapshot", || self.close_inner())
    }
}

impl Drop for NativePgSnapshotSession {
    fn drop(&mut self) {
        if self.client.is_some() {
            let _ = run_pg_thread("drop PostgreSQL snapshot", || self.close_inner());
        }
    }
}

#[derive(Default)]
pub struct UnsupportedPgSourceDriver;

impl PgSourceDriver for UnsupportedPgSourceDriver {
    fn probe(
        &self,
        _endpoint: &ResolvedPgEndpoint,
        _credential: &ResolvedCredential,
    ) -> Result<PgProbeStatus> {
        Err(CnxError::NotSupported(
            "pg-source PostgreSQL driver is not linked in this runtime artifact".into(),
        ))
    }

    fn discover_fields(
        &self,
        _endpoint: &ResolvedPgEndpoint,
        _credential: &ResolvedCredential,
    ) -> Result<PgFieldCatalog> {
        Err(CnxError::NotSupported(
            "pg-source field discovery requires PostgreSQL driver support".into(),
        ))
    }

    fn open_snapshot(
        &self,
        _endpoint: &ResolvedPgEndpoint,
        _credential: &ResolvedCredential,
        _schema: &str,
        _table: &str,
        _fields: &[String],
    ) -> Result<Box<dyn PgSnapshotSession>> {
        Err(CnxError::NotSupported(
            "pg-source snapshot requires PostgreSQL driver support".into(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PgKeyType {
    cast_type: String,
    value_kind: PgKeyValueKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PgKeyValueKind {
    Integer,
    Float,
    Bool,
    String,
}

fn run_pg_thread<T, F>(context: &'static str, operation: F) -> Result<T>
where
    T: Send,
    F: FnOnce() -> Result<T> + Send,
{
    std::thread::scope(|scope| {
        scope
            .spawn(operation)
            .join()
            .map_err(|_| CnxError::Internal(format!("{context}: PostgreSQL thread panicked")))?
    })
}

fn open_client(
    endpoint: &ResolvedPgEndpoint,
    credential: &ResolvedCredential,
    timeout: Option<Duration>,
) -> Result<Client> {
    let mut url = Url::parse(&endpoint.endpoint_uri).map_err(|err| {
        CnxError::InvalidInput(format!(
            "invalid pg endpoint_uri '{}': {err}",
            endpoint.endpoint_uri
        ))
    })?;
    match url.scheme() {
        "pg" | "postgres" | "postgresql" => {}
        other => {
            return Err(CnxError::InvalidInput(format!(
                "unsupported pg endpoint_uri scheme '{other}'"
            )));
        }
    }
    url.host_str().ok_or_else(|| {
        CnxError::InvalidInput(format!(
            "pg endpoint_uri '{}' missing host",
            endpoint.endpoint_uri
        ))
    })?;
    if !url.username().is_empty() || url.password().is_some() {
        return Err(CnxError::InvalidInput(
            "pg endpoint_uri must not contain credential material".into(),
        ));
    }
    url.set_scheme("postgresql").map_err(|_| {
        CnxError::InvalidInput(format!(
            "invalid pg endpoint_uri scheme in '{}'",
            endpoint.endpoint_uri
        ))
    })?;
    let mut config = Config::from_str(url.as_str()).map_err(|err| {
        CnxError::InvalidInput(format!(
            "invalid PostgreSQL connection configuration: {err}"
        ))
    })?;
    if let Some(timeout) = timeout {
        config.connect_timeout(nonzero_timeout(timeout));
    }
    match credential {
        ResolvedCredential::None => {}
        ResolvedCredential::Basic { username, password } => {
            config.user(username);
            config.password(password);
        }
        ResolvedCredential::ApiKey { .. } | ResolvedCredential::Bearer { .. } => {
            return Err(CnxError::InvalidInput(
                "pg-source supports basic_env credentials for PostgreSQL connections".into(),
            ));
        }
    }
    if config.get_ssl_mode() == SslMode::Disable {
        return config
            .connect(NoTls)
            .map_err(pg_error("connect PostgreSQL endpoint"));
    }
    let connector = TlsConnector::builder()
        .build()
        .map(MakeTlsConnector::new)
        .map_err(|err| CnxError::LinkError(format!("build PostgreSQL TLS connector: {err}")))?;
    config
        .connect(connector)
        .map_err(pg_error("connect PostgreSQL TLS endpoint"))
}

fn set_statement_timeout(
    client: &mut Client,
    timeout: Option<Duration>,
    transaction_local: bool,
) -> Result<()> {
    let Some(timeout) = timeout else {
        return Ok(());
    };
    let scope = if transaction_local {
        "SET LOCAL"
    } else {
        "SET"
    };
    client
        .batch_execute(&format!(
            "{scope} statement_timeout = {}",
            nonzero_timeout(timeout).as_millis()
        ))
        .map_err(pg_error("set PostgreSQL statement timeout"))
}

fn nonzero_timeout(timeout: Duration) -> Duration {
    timeout.max(Duration::from_millis(1))
}

fn load_table_fields(client: &mut Client, schema: &str, table: &str) -> Result<Vec<String>> {
    client
        .query(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position",
            &[&schema, &table],
        )
        .map_err(pg_error("discover PostgreSQL table fields"))?
        .into_iter()
        .map(|row| {
            row.try_get::<_, String>(0)
                .map_err(pg_error("decode PostgreSQL table field"))
        })
        .collect()
}

fn load_single_primary_key(
    client: &mut Client,
    schema: &str,
    table: &str,
) -> Result<Option<String>> {
    let mut fields = client
        .query(
            "SELECT kcu.column_name \
         FROM information_schema.table_constraints tc \
         JOIN information_schema.key_column_usage kcu \
           ON tc.constraint_catalog = kcu.constraint_catalog \
          AND tc.constraint_schema = kcu.constraint_schema \
          AND tc.constraint_name = kcu.constraint_name \
         WHERE tc.table_schema = $1 \
           AND tc.table_name = $2 \
           AND tc.constraint_type = 'PRIMARY KEY' \
         ORDER BY kcu.ordinal_position",
            &[&schema, &table],
        )
        .map_err(pg_error("discover PostgreSQL primary key"))?
        .into_iter()
        .map(|row| {
            row.try_get::<_, String>(0)
                .map_err(pg_error("decode PostgreSQL primary key"))
        })
        .collect::<Result<Vec<_>>>()?;
    match fields.len() {
        0 => Ok(None),
        1 => Ok(fields.pop()),
        _ => Err(CnxError::InvalidInput(format!(
            "pg snapshot does not support composite primary keys for `{schema}`.`{table}`"
        ))),
    }
}

fn load_key_type(
    client: &mut Client,
    schema: &str,
    table: &str,
    key_field: &str,
) -> Result<PgKeyType> {
    let row = client
        .query(
            "SELECT data_type, udt_name FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2 AND column_name = $3",
            &[&schema, &table, &key_field],
        )
        .map_err(pg_error("discover PostgreSQL key type"))?
        .into_iter()
        .next()
        .ok_or_else(|| CnxError::InvalidInput(format!("pg key field '{key_field}' not found")))?;
    let data_type = row
        .try_get::<_, String>(0)
        .map_err(pg_error("decode PostgreSQL key data type"))?;
    let udt_name = row
        .try_get::<_, String>(1)
        .map_err(pg_error("decode PostgreSQL key UDT name"))?;
    let cast_type = sanitize_pg_type_name(&udt_name)?;
    let normalized = data_type.to_ascii_lowercase();
    let value_kind = match normalized.as_str() {
        "smallint" | "integer" | "bigint" => PgKeyValueKind::Integer,
        "numeric" | "real" | "double precision" => PgKeyValueKind::Float,
        "boolean" => PgKeyValueKind::Bool,
        _ => PgKeyValueKind::String,
    };
    Ok(PgKeyType {
        cast_type,
        value_kind,
    })
}

fn validate_requested_fields(fields: &[String], table_fields: &[String]) -> Result<()> {
    for field in fields {
        if !table_fields.iter().any(|candidate| candidate == field) {
            return Err(CnxError::InvalidInput(format!(
                "requested PostgreSQL field '{field}' is not present in table"
            )));
        }
    }
    Ok(())
}

fn row_to_event_and_key(
    output_fields: &[String],
    key_type: &PgKeyType,
    json_text: &str,
    key_text: &str,
) -> Result<(PgRowEvent, Value)> {
    let mut object = match serde_json::from_str::<Value>(json_text).map_err(|err| {
        CnxError::PeerError(format!("decode PostgreSQL row_to_json payload: {err}"))
    })? {
        Value::Object(object) => object,
        other => {
            return Err(CnxError::PeerError(format!(
                "PostgreSQL row_to_json returned non-object payload {other:?}"
            )));
        }
    };
    filter_output_fields(&mut object, output_fields);
    Ok((
        PgRowEvent {
            values: Value::Object(object),
        },
        key_text_to_json(key_text, key_type)?,
    ))
}

fn filter_output_fields(object: &mut Map<String, Value>, output_fields: &[String]) {
    object.retain(|key, _| output_fields.iter().any(|field| field == key));
}

fn key_text_to_json(value: &str, key_type: &PgKeyType) -> Result<Value> {
    Ok(match key_type.value_kind {
        PgKeyValueKind::Integer => {
            let value = value.parse::<i64>().map_err(|err| {
                CnxError::PeerError(format!("decode PostgreSQL integer key '{value}': {err}"))
            })?;
            Value::Number(Number::from(value))
        }
        PgKeyValueKind::Float => {
            let value = value.parse::<f64>().map_err(|err| {
                CnxError::PeerError(format!("decode PostgreSQL numeric key '{value}': {err}"))
            })?;
            Number::from_f64(value)
                .map(Value::Number)
                .ok_or_else(|| CnxError::PeerError("PostgreSQL key is not finite".into()))?
        }
        PgKeyValueKind::Bool => Value::Bool(matches!(value, "t" | "true" | "1")),
        PgKeyValueKind::String => Value::String(value.to_string()),
    })
}

fn key_value_to_param_string(value: &Value) -> Result<String> {
    Ok(match value {
        Value::Null => {
            return Err(CnxError::InvalidInput(
                "pg snapshot cursor last_key must not be null".into(),
            ));
        }
        Value::Bool(value) => value.to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(_) | Value::Object(_) => {
            return Err(CnxError::InvalidInput(
                "pg snapshot cursor last_key must be scalar".into(),
            ));
        }
    })
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn pg_error(context: &'static str) -> impl Fn(postgres::Error) -> CnxError {
    move |error| {
        let message = format!("{context}: {error}");
        let Some(db_error) = error.as_db_error() else {
            return if error.is_closed() {
                CnxError::LinkError(message)
            } else {
                CnxError::PeerError(message)
            };
        };
        let code = db_error.code().code();
        if code.starts_with("08") {
            CnxError::LinkError(message)
        } else if matches!(code, "28000" | "28P01" | "42501") {
            CnxError::AccessDenied(message)
        } else if matches!(code, "42P01" | "42703" | "3F000" | "22P02") {
            CnxError::InvalidInput(message)
        } else if code == "57014" {
            CnxError::Timeout
        } else {
            CnxError::PeerError(message)
        }
    }
}

fn sanitize_pg_type_name(value: &str) -> Result<String> {
    if !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '.')
    {
        Ok(value.to_string())
    } else {
        Err(CnxError::InvalidInput(format!(
            "unsupported PostgreSQL key type name '{value}'"
        )))
    }
}

fn scope_contains(value: &str, scopes: &[String]) -> bool {
    scopes.iter().any(|scope| {
        scope == "*"
            || scope == value
            || scope.split_once('*').is_some_and(|(prefix, suffix)| {
                value.starts_with(prefix) && value.ends_with(suffix)
            })
    })
}

fn nonempty(value: String) -> Option<String> {
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn json_cursor_key_converts_supported_scalars_to_param_strings() {
        assert_eq!(key_value_to_param_string(&json!(7)).unwrap(), "7");
        assert_eq!(key_value_to_param_string(&json!("key-7")).unwrap(), "key-7");
        assert_eq!(key_value_to_param_string(&json!(true)).unwrap(), "true");
    }

    #[test]
    fn json_cursor_key_rejects_null_and_structured_values() {
        assert!(
            key_value_to_param_string(&serde_json::Value::Null)
                .unwrap_err()
                .to_string()
                .contains("must not be null")
        );
        assert!(
            key_value_to_param_string(&json!({"id": 1}))
                .unwrap_err()
                .to_string()
                .contains("must be scalar")
        );
    }

    #[test]
    fn key_text_decodes_common_types() {
        let integer = PgKeyType {
            cast_type: "int8".into(),
            value_kind: PgKeyValueKind::Integer,
        };
        let text = PgKeyType {
            cast_type: "text".into(),
            value_kind: PgKeyValueKind::String,
        };

        assert_eq!(key_text_to_json("42", &integer).unwrap(), json!(42));
        assert_eq!(key_text_to_json("abc", &text).unwrap(), json!("abc"));
    }

    #[test]
    fn identifier_quoting_escapes_double_quotes() {
        assert_eq!(quote_identifier("a\"b"), "\"a\"\"b\"");
    }
}
