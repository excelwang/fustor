use std::time::Duration;

use capanix_app_sdk::{CnxError, Result};
use mysql::consts::{ColumnFlags, ColumnType};
use mysql::prelude::Queryable;
use mysql::{Column, Conn, Opts, OptsBuilder, Row, SslOpts, Value as MysqlValue, from_row_opt};
use mysql_source::shared_types::{
    MysqlFieldCatalog, MysqlFieldDescriptor, MysqlProbeStatus, MysqlRowEvent,
};
use mysql_source::{ResolvedCredential, ResolvedMysqlEndpoint};
use serde_json::{Map, Number, Value};
use source_kit::SourceOperationBudget;
use url::Url;

pub trait MysqlSourceDriver: Send + Sync {
    fn probe(
        &self,
        endpoint: &ResolvedMysqlEndpoint,
        credential: &ResolvedCredential,
    ) -> Result<MysqlProbeStatus>;

    fn probe_with_timeout(
        &self,
        endpoint: &ResolvedMysqlEndpoint,
        credential: &ResolvedCredential,
        timeout: Option<Duration>,
    ) -> Result<MysqlProbeStatus> {
        let _ = timeout;
        self.probe(endpoint, credential)
    }

    fn probe_with_budget(
        &self,
        endpoint: &ResolvedMysqlEndpoint,
        credential: &ResolvedCredential,
        budget: &mut SourceOperationBudget,
    ) -> Result<MysqlProbeStatus> {
        budget.observe_fanout(1)?;
        budget.consume_call(1)?;
        self.probe_with_timeout(endpoint, credential, budget.remaining_timeout()?)
    }

    fn discover_fields(
        &self,
        endpoint: &ResolvedMysqlEndpoint,
        credential: &ResolvedCredential,
    ) -> Result<MysqlFieldCatalog>;

    fn discover_fields_with_timeout(
        &self,
        endpoint: &ResolvedMysqlEndpoint,
        credential: &ResolvedCredential,
        timeout: Option<Duration>,
    ) -> Result<MysqlFieldCatalog> {
        let _ = timeout;
        self.discover_fields(endpoint, credential)
    }

    fn discover_fields_with_budget(
        &self,
        endpoint: &ResolvedMysqlEndpoint,
        credential: &ResolvedCredential,
        budget: &mut SourceOperationBudget,
    ) -> Result<MysqlFieldCatalog> {
        budget.observe_fanout(1)?;
        budget.consume_call(1)?;
        self.discover_fields_with_timeout(endpoint, credential, budget.remaining_timeout()?)
    }

    fn open_snapshot(
        &self,
        endpoint: &ResolvedMysqlEndpoint,
        credential: &ResolvedCredential,
        schema: &str,
        table: &str,
        fields: &[String],
    ) -> Result<Box<dyn MysqlSnapshotSession>>;

    fn open_snapshot_with_timeout(
        &self,
        endpoint: &ResolvedMysqlEndpoint,
        credential: &ResolvedCredential,
        schema: &str,
        table: &str,
        fields: &[String],
        timeout: Option<Duration>,
    ) -> Result<Box<dyn MysqlSnapshotSession>> {
        let _ = timeout;
        self.open_snapshot(endpoint, credential, schema, table, fields)
    }

    fn open_snapshot_with_budget(
        &self,
        endpoint: &ResolvedMysqlEndpoint,
        credential: &ResolvedCredential,
        schema: &str,
        table: &str,
        fields: &[String],
        budget: &mut SourceOperationBudget,
    ) -> Result<Box<dyn MysqlSnapshotSession>> {
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

pub trait MysqlSnapshotSession: Send {
    fn fetch_page(&mut self, limit: usize, last_key: Option<&Value>) -> Result<MysqlSnapshotPage>;
    fn fetch_page_with_timeout(
        &mut self,
        limit: usize,
        last_key: Option<&Value>,
        timeout: Option<Duration>,
    ) -> Result<MysqlSnapshotPage> {
        let _ = timeout;
        self.fetch_page(limit, last_key)
    }
    fn fetch_page_with_budget(
        &mut self,
        limit: usize,
        last_key: Option<&Value>,
        budget: &mut SourceOperationBudget,
    ) -> Result<MysqlSnapshotPage> {
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
pub struct MysqlSnapshotPage {
    pub fields: Vec<String>,
    pub rows: Vec<MysqlRowEvent>,
    pub source_revision: String,
    pub last_key: Option<Value>,
    pub has_more: bool,
}

#[derive(Default)]
pub struct NativeMysqlSourceDriver;

impl MysqlSourceDriver for NativeMysqlSourceDriver {
    fn probe(
        &self,
        endpoint: &ResolvedMysqlEndpoint,
        credential: &ResolvedCredential,
    ) -> Result<MysqlProbeStatus> {
        self.probe_with_timeout(endpoint, credential, None)
    }

    fn probe_with_timeout(
        &self,
        endpoint: &ResolvedMysqlEndpoint,
        credential: &ResolvedCredential,
        timeout: Option<Duration>,
    ) -> Result<MysqlProbeStatus> {
        let mut conn = open_connection(endpoint, credential, timeout)?;
        set_mysql_query_timeout(&mut conn, timeout)?;
        let server_version: Option<String> = conn
            .query_first("SELECT VERSION()")
            .map_err(mysql_error("probe mysql endpoint"))?;
        Ok(MysqlProbeStatus {
            object_ref: endpoint.object_ref.clone(),
            endpoint_uri: endpoint.endpoint_uri.clone(),
            reachable: true,
            server_version,
            diagnostics: None,
        })
    }

    fn discover_fields(
        &self,
        endpoint: &ResolvedMysqlEndpoint,
        credential: &ResolvedCredential,
    ) -> Result<MysqlFieldCatalog> {
        self.discover_fields_with_timeout(endpoint, credential, None)
    }

    fn discover_fields_with_timeout(
        &self,
        endpoint: &ResolvedMysqlEndpoint,
        credential: &ResolvedCredential,
        timeout: Option<Duration>,
    ) -> Result<MysqlFieldCatalog> {
        let mut conn = open_connection(endpoint, credential, timeout)?;
        set_mysql_query_timeout(&mut conn, timeout)?;
        let sql = r#"
SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
"#;
        let rows: Vec<Row> = conn
            .query(sql)
            .map_err(mysql_error("discover mysql fields"))?;
        let mut fields = Vec::with_capacity(rows.len());
        for row in rows {
            let (schema, table, column, ordinal_position, column_type): (
                String,
                String,
                String,
                Option<u64>,
                Option<String>,
            ) = from_row_opt(row)
                .map_err(|err| CnxError::PeerError(format!("decode mysql field row: {err}")))?;
            if scope_contains(&schema, &endpoint.schema_scopes)
                && scope_contains(&table, &endpoint.table_scopes)
            {
                fields.push(MysqlFieldDescriptor {
                    schema,
                    table,
                    column,
                    ordinal_position,
                    column_type,
                });
            }
        }
        Ok(MysqlFieldCatalog {
            object_ref: endpoint.object_ref.clone(),
            fields,
        })
    }

    fn open_snapshot(
        &self,
        endpoint: &ResolvedMysqlEndpoint,
        credential: &ResolvedCredential,
        schema: &str,
        table: &str,
        fields: &[String],
    ) -> Result<Box<dyn MysqlSnapshotSession>> {
        self.open_snapshot_with_timeout(endpoint, credential, schema, table, fields, None)
    }

    fn open_snapshot_with_timeout(
        &self,
        endpoint: &ResolvedMysqlEndpoint,
        credential: &ResolvedCredential,
        schema: &str,
        table: &str,
        fields: &[String],
        timeout: Option<Duration>,
    ) -> Result<Box<dyn MysqlSnapshotSession>> {
        let mut conn = open_connection(endpoint, credential, timeout)?;
        set_mysql_query_timeout(&mut conn, timeout)?;
        conn.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .map_err(mysql_error("set mysql snapshot isolation"))?;
        conn.query_drop("SET TRANSACTION READ ONLY")
            .map_err(mysql_error("set mysql snapshot read-only mode"))?;
        conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            .map_err(mysql_error("start mysql snapshot transaction"))?;
        let snapshot_revision: String = conn
            .query_first(
                "SELECT CONCAT(@@GLOBAL.server_uuid, ':', COALESCE(@@GLOBAL.gtid_executed, ''), ':', CONNECTION_ID(), ':', UUID())",
            )
            .map_err(mysql_error("read mysql transaction snapshot revision"))?
            .ok_or_else(|| {
                CnxError::PeerError("mysql transaction snapshot revision is unavailable".into())
            })?;

        let table_fields = load_table_fields(&mut conn, schema, table)?;
        let output_fields = if fields.is_empty() {
            table_fields.clone()
        } else {
            validate_requested_fields(fields, &table_fields)?;
            fields.to_vec()
        };
        if output_fields.is_empty() {
            return Err(CnxError::InvalidInput(format!(
                "mysql table `{schema}`.`{table}` has no selectable fields"
            )));
        }
        let key_field = match endpoint.primary_key.clone() {
            Some(primary_key) => primary_key,
            None => load_single_primary_key(&mut conn, schema, table)?.ok_or_else(|| {
                CnxError::InvalidInput(format!(
                    "mysql snapshot requires a configured or discoverable single-column primary_key for `{schema}`.`{table}`"
                ))
            })?,
        };
        if !table_fields.iter().any(|field| field == &key_field) {
            return Err(CnxError::InvalidInput(format!(
                "mysql primary_key '{key_field}' is not a column of `{schema}`.`{table}`"
            )));
        }
        let mut select_fields = output_fields.clone();
        if !select_fields.iter().any(|field| field == &key_field) {
            select_fields.push(key_field.clone());
        }
        Ok(Box::new(NativeMysqlSnapshotSession {
            conn,
            schema: schema.to_string(),
            table: table.to_string(),
            output_fields,
            select_fields,
            key_field,
            source_revision: format!("mysql-consistent-snapshot:{snapshot_revision}"),
            closed: false,
        }))
    }
}

struct NativeMysqlSnapshotSession {
    conn: Conn,
    schema: String,
    table: String,
    output_fields: Vec<String>,
    select_fields: Vec<String>,
    key_field: String,
    source_revision: String,
    closed: bool,
}

impl MysqlSnapshotSession for NativeMysqlSnapshotSession {
    fn fetch_page(&mut self, limit: usize, last_key: Option<&Value>) -> Result<MysqlSnapshotPage> {
        self.fetch_page_with_timeout(limit, last_key, None)
    }

    fn fetch_page_with_timeout(
        &mut self,
        limit: usize,
        last_key: Option<&Value>,
        timeout: Option<Duration>,
    ) -> Result<MysqlSnapshotPage> {
        if self.closed {
            return Err(CnxError::NotReady(
                "mysql snapshot session is closed".into(),
            ));
        }
        set_mysql_query_timeout(&mut self.conn, timeout)?;
        let fetch_limit = limit.saturating_add(1);
        let field_list = self
            .select_fields
            .iter()
            .map(|field| quote_identifier(field))
            .collect::<Vec<_>>()
            .join(", ");
        let mut rows: Vec<Row> = match last_key {
            Some(last_key) => {
                let sql = format!(
                    "SELECT {field_list} FROM {}.{} WHERE {} > ? ORDER BY {} LIMIT {}",
                    quote_identifier(&self.schema),
                    quote_identifier(&self.table),
                    quote_identifier(&self.key_field),
                    quote_identifier(&self.key_field),
                    fetch_limit,
                );
                self.conn
                    .exec(sql, (json_to_mysql_value(last_key)?,))
                    .map_err(mysql_error("fetch mysql snapshot page"))?
            }
            None => {
                let sql = format!(
                    "SELECT {field_list} FROM {}.{} ORDER BY {} LIMIT {}",
                    quote_identifier(&self.schema),
                    quote_identifier(&self.table),
                    quote_identifier(&self.key_field),
                    fetch_limit,
                );
                self.conn
                    .query(sql)
                    .map_err(mysql_error("fetch mysql snapshot page"))?
            }
        };
        let has_more = rows.len() > limit;
        if has_more {
            rows.truncate(limit);
        }
        let mut events = Vec::with_capacity(rows.len());
        let mut page_last_key = None;
        for row in rows {
            let (event, row_key) = row_to_event_and_key(
                &self.select_fields,
                &self.output_fields,
                &self.key_field,
                row,
            )?;
            page_last_key = row_key;
            events.push(event);
        }
        Ok(MysqlSnapshotPage {
            fields: self.output_fields.clone(),
            rows: events,
            source_revision: self.source_revision.clone(),
            last_key: page_last_key,
            has_more,
        })
    }

    fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.conn
            .query_drop("ROLLBACK")
            .map_err(mysql_error("close mysql snapshot transaction"))
    }
}

impl Drop for NativeMysqlSnapshotSession {
    fn drop(&mut self) {
        if !self.closed {
            let _ = self.conn.query_drop("ROLLBACK");
        }
    }
}

#[derive(Default)]
pub struct UnsupportedMysqlSourceDriver;

impl MysqlSourceDriver for UnsupportedMysqlSourceDriver {
    fn probe(
        &self,
        _endpoint: &ResolvedMysqlEndpoint,
        _credential: &ResolvedCredential,
    ) -> Result<MysqlProbeStatus> {
        Err(CnxError::NotSupported(
            "mysql-source real MySQL client is not linked in this runtime artifact".into(),
        ))
    }

    fn discover_fields(
        &self,
        _endpoint: &ResolvedMysqlEndpoint,
        _credential: &ResolvedCredential,
    ) -> Result<MysqlFieldCatalog> {
        Err(CnxError::NotSupported(
            "mysql-source field discovery requires a linked MySQL client".into(),
        ))
    }

    fn open_snapshot(
        &self,
        _endpoint: &ResolvedMysqlEndpoint,
        _credential: &ResolvedCredential,
        _schema: &str,
        _table: &str,
        _fields: &[String],
    ) -> Result<Box<dyn MysqlSnapshotSession>> {
        Err(CnxError::NotSupported(
            "mysql-source snapshot requires a linked MySQL client".into(),
        ))
    }
}

fn open_connection(
    endpoint: &ResolvedMysqlEndpoint,
    credential: &ResolvedCredential,
    timeout: Option<Duration>,
) -> Result<Conn> {
    let url = Url::parse(&endpoint.endpoint_uri).map_err(|err| {
        CnxError::InvalidInput(format!(
            "invalid mysql endpoint_uri '{}': {err}",
            endpoint.endpoint_uri
        ))
    })?;
    let host = url.host_str().ok_or_else(|| {
        CnxError::InvalidInput(format!(
            "mysql endpoint_uri '{}' missing host",
            endpoint.endpoint_uri
        ))
    })?;
    let port = url.port().unwrap_or(3306);
    let database = url.path().trim_start_matches('/').trim();
    let mut builder = OptsBuilder::new()
        .ip_or_hostname(Some(host.to_string()))
        .tcp_port(port);
    if let Some(timeout) = timeout {
        let timeout = nonzero_timeout(timeout);
        builder = builder
            .tcp_connect_timeout(Some(timeout))
            .read_timeout(Some(timeout))
            .write_timeout(Some(timeout));
    }
    if !database.is_empty() {
        builder = builder.db_name(Some(database.to_string()));
    }
    match credential {
        ResolvedCredential::None => {}
        ResolvedCredential::Basic { username, password } => {
            builder = builder
                .user(Some(username.clone()))
                .pass(Some(password.clone()));
        }
        ResolvedCredential::ApiKey { .. } | ResolvedCredential::Bearer { .. } => {
            return Err(CnxError::InvalidInput(
                "mysql-source supports basic_env credentials for MySQL connections".into(),
            ));
        }
    }
    if endpoint.tls.is_some()
        || url.query_pairs().any(|(key, value)| {
            key.eq_ignore_ascii_case("ssl") && value.eq_ignore_ascii_case("true")
        })
    {
        let mut tls = SslOpts::default();
        if let Some(ca_file) = endpoint.tls.as_ref().and_then(|tls| tls.ca_file.as_ref()) {
            tls = tls.with_root_cert_path(Some(std::path::PathBuf::from(ca_file)));
        }
        builder = builder.ssl_opts(Some(tls));
    }
    Conn::new(Opts::from(builder)).map_err(mysql_error("connect mysql endpoint"))
}

fn set_mysql_query_timeout(conn: &mut Conn, timeout: Option<Duration>) -> Result<()> {
    let Some(timeout) = timeout else {
        return Ok(());
    };
    conn.query_drop(format!(
        "SET SESSION MAX_EXECUTION_TIME = {}",
        nonzero_timeout(timeout).as_millis()
    ))
    .map_err(mysql_error("set mysql query timeout"))
}

fn nonzero_timeout(timeout: Duration) -> Duration {
    timeout.max(Duration::from_millis(1))
}

fn load_table_fields(conn: &mut Conn, schema: &str, table: &str) -> Result<Vec<String>> {
    let rows: Vec<Row> = conn
        .exec(
            r#"
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
ORDER BY ORDINAL_POSITION
"#,
            (schema, table),
        )
        .map_err(mysql_error("discover mysql table fields"))?;
    let mut fields = Vec::with_capacity(rows.len());
    for row in rows {
        let column: String = from_row_opt(row)
            .map_err(|err| CnxError::PeerError(format!("decode mysql column row: {err}")))?;
        fields.push(column);
    }
    Ok(fields)
}

fn load_single_primary_key(conn: &mut Conn, schema: &str, table: &str) -> Result<Option<String>> {
    let rows: Vec<Row> = conn
        .exec(
            r#"
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
ORDER BY ORDINAL_POSITION
"#,
            (schema, table),
        )
        .map_err(mysql_error("discover mysql primary key"))?;
    let mut fields = Vec::with_capacity(rows.len());
    for row in rows {
        let column: String = from_row_opt(row)
            .map_err(|err| CnxError::PeerError(format!("decode mysql primary key row: {err}")))?;
        fields.push(column);
    }
    match fields.len() {
        0 => Ok(None),
        1 => Ok(fields.pop()),
        _ => Err(CnxError::InvalidInput(format!(
            "mysql snapshot does not support composite primary keys for `{schema}`.`{table}`"
        ))),
    }
}

fn validate_requested_fields(fields: &[String], table_fields: &[String]) -> Result<()> {
    for field in fields {
        if !table_fields.iter().any(|candidate| candidate == field) {
            return Err(CnxError::InvalidInput(format!(
                "requested MySQL field '{field}' is not present in table"
            )));
        }
    }
    Ok(())
}

fn row_to_event_and_key(
    select_fields: &[String],
    output_fields: &[String],
    key_field: &str,
    row: Row,
) -> Result<(MysqlRowEvent, Option<Value>)> {
    let columns = row.columns_ref().to_vec();
    let values = row.unwrap();
    let mut object = Map::with_capacity(output_fields.len());
    let mut last_key = None;
    for ((field, value), column) in select_fields.iter().zip(values).zip(columns.iter()) {
        let json_value = mysql_value_to_json(value, column)?;
        if field == key_field {
            last_key = Some(json_value.clone());
        }
        if output_fields.iter().any(|candidate| candidate == field) {
            object.insert(field.clone(), json_value);
        }
    }
    Ok((
        MysqlRowEvent {
            values: Value::Object(object),
        },
        last_key,
    ))
}

fn mysql_value_to_json(value: MysqlValue, column: &Column) -> Result<Value> {
    Ok(match value {
        MysqlValue::NULL => Value::Null,
        MysqlValue::Bytes(bytes) => mysql_bytes_to_json(bytes, column)?,
        MysqlValue::Int(value) => Value::Number(Number::from(value)),
        MysqlValue::UInt(value) => Value::Number(Number::from(value)),
        MysqlValue::Float(value) => Number::from_f64(value as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        MysqlValue::Double(value) => Number::from_f64(value)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        MysqlValue::Date(year, month, day, hour, minute, second, micros) => Value::String(format!(
            "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{micros:06}Z"
        )),
        MysqlValue::Time(is_neg, days, hours, minutes, seconds, micros) => {
            let sign = if is_neg { "-" } else { "" };
            Value::String(format!(
                "{sign}{days}d {hours:02}:{minutes:02}:{seconds:02}.{micros:06}"
            ))
        }
    })
}

fn mysql_bytes_to_json(bytes: Vec<u8>, column: &Column) -> Result<Value> {
    use ColumnType::*;

    let text = String::from_utf8(bytes.clone()).ok();
    match column.column_type() {
        MYSQL_TYPE_TINY | MYSQL_TYPE_SHORT | MYSQL_TYPE_INT24 | MYSQL_TYPE_LONG
        | MYSQL_TYPE_LONGLONG | MYSQL_TYPE_YEAR => {
            let text = text.ok_or_else(|| {
                CnxError::PeerError("decode MySQL integer column as UTF-8".into())
            })?;
            if column.flags().contains(ColumnFlags::UNSIGNED_FLAG) {
                text.parse::<u64>()
                    .map(Number::from)
                    .map(Value::Number)
                    .map_err(|err| {
                        CnxError::PeerError(format!(
                            "decode MySQL unsigned integer '{text}': {err}"
                        ))
                    })
            } else {
                text.parse::<i64>()
                    .map(Number::from)
                    .map(Value::Number)
                    .map_err(|err| {
                        CnxError::PeerError(format!("decode MySQL integer '{text}': {err}"))
                    })
            }
        }
        MYSQL_TYPE_FLOAT | MYSQL_TYPE_DOUBLE => {
            let text = text.ok_or_else(|| {
                CnxError::PeerError("decode MySQL floating-point column as UTF-8".into())
            })?;
            let number = text.parse::<f64>().map_err(|err| {
                CnxError::PeerError(format!("decode MySQL floating-point '{text}': {err}"))
            })?;
            Number::from_f64(number).map(Value::Number).ok_or_else(|| {
                CnxError::PeerError("MySQL floating-point value is not finite".into())
            })
        }
        MYSQL_TYPE_JSON => {
            let text = text
                .ok_or_else(|| CnxError::PeerError("decode MySQL JSON column as UTF-8".into()))?;
            serde_json::from_str(&text)
                .map_err(|err| CnxError::PeerError(format!("decode MySQL JSON column: {err}")))
        }
        MYSQL_TYPE_BIT => Ok(Value::Number(Number::from(
            bytes
                .into_iter()
                .fold(0_u64, |value, byte| (value << 8) | u64::from(byte)),
        ))),
        MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => text
            .map(Value::String)
            .ok_or_else(|| CnxError::PeerError("decode MySQL decimal column as UTF-8".into())),
        _ if column.flags().contains(ColumnFlags::BINARY_FLAG) || column.character_set() == 63 => {
            Ok(Value::String(base64_encode(&bytes)))
        }
        _ => Ok(match text {
            Some(text) => Value::String(text),
            None => Value::String(base64_encode(&bytes)),
        }),
    }
}

fn json_to_mysql_value(value: &Value) -> Result<MysqlValue> {
    Ok(match value {
        Value::Null => {
            return Err(CnxError::InvalidInput(
                "mysql snapshot cursor last_key must not be null".into(),
            ));
        }
        Value::Bool(value) => MysqlValue::Int(if *value { 1 } else { 0 }),
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                MysqlValue::Int(value)
            } else if let Some(value) = number.as_u64() {
                MysqlValue::UInt(value)
            } else if let Some(value) = number.as_f64() {
                MysqlValue::Double(value)
            } else {
                return Err(CnxError::InvalidInput(
                    "mysql snapshot cursor last_key number is unsupported".into(),
                ));
            }
        }
        Value::String(value) => MysqlValue::Bytes(value.clone().into_bytes()),
        Value::Array(_) | Value::Object(_) => {
            return Err(CnxError::InvalidInput(
                "mysql snapshot cursor last_key must be scalar".into(),
            ));
        }
    })
}

fn quote_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
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

fn mysql_error(context: &'static str) -> impl Fn(mysql::Error) -> CnxError {
    move |err| match &err {
        mysql::Error::IoError(io) if io.kind() == std::io::ErrorKind::TimedOut => CnxError::Timeout,
        mysql::Error::IoError(_) | mysql::Error::CodecError(_) | mysql::Error::DriverError(_) => {
            CnxError::LinkError(format!("{context}: {err}"))
        }
        mysql::Error::MySqlError(server) if server.code == 3024 => CnxError::Timeout,
        mysql::Error::MySqlError(server) if matches!(server.code, 1044 | 1045 | 1142 | 1227) => {
            CnxError::AccessDenied(format!("{context}: mysql access denied"))
        }
        mysql::Error::MySqlError(server) if matches!(server.code, 1205 | 1213) => {
            CnxError::LinkError(format!("{context}: mysql transient server failure"))
        }
        _ => CnxError::PeerError(format!("{context}: {err}")),
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        out.push(TABLE[(b0 >> 2) as usize] as char);
        out.push(TABLE[(((b0 & 0b0000_0011) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[(((b1 & 0b0000_1111) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(b2 & 0b0011_1111) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use mysql::{MySqlError, Value as MysqlValue};
    use serde_json::json;

    use super::*;

    #[test]
    fn json_cursor_key_converts_supported_scalars_to_mysql_values() {
        assert_eq!(json_to_mysql_value(&json!(7)).unwrap(), MysqlValue::Int(7));
        assert_eq!(
            json_to_mysql_value(&json!("key-7")).unwrap(),
            MysqlValue::Bytes(b"key-7".to_vec())
        );
        assert_eq!(
            json_to_mysql_value(&json!(true)).unwrap(),
            MysqlValue::Int(1)
        );
    }

    #[test]
    fn json_cursor_key_rejects_null_and_structured_values() {
        assert!(
            json_to_mysql_value(&serde_json::Value::Null)
                .unwrap_err()
                .to_string()
                .contains("must not be null")
        );
        assert!(
            json_to_mysql_value(&json!({"id": 1}))
                .unwrap_err()
                .to_string()
                .contains("must be scalar")
        );
    }

    #[test]
    fn mysql_errors_preserve_timeout_access_and_retry_classes() {
        let timeout = mysql_error("test")(mysql::Error::IoError(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "timed out",
        )));
        assert!(matches!(timeout, CnxError::Timeout));

        let access = mysql_error("test")(mysql::Error::MySqlError(MySqlError {
            state: "28000".into(),
            message: "denied".into(),
            code: 1045,
        }));
        assert!(matches!(access, CnxError::AccessDenied(_)));

        let retryable = mysql_error("test")(mysql::Error::MySqlError(MySqlError {
            state: "40001".into(),
            message: "deadlock".into(),
            code: 1213,
        }));
        assert!(matches!(retryable, CnxError::LinkError(_)));
    }
}
