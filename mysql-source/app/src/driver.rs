use capanix_app_sdk::{CnxError, Result};
use mysql::prelude::Queryable;
use mysql::{Conn, Opts, OptsBuilder, Row, SslOpts, Value as MysqlValue, from_row_opt};
use mysql_source::shared_types::{
    MysqlFieldCatalog, MysqlFieldDescriptor, MysqlProbeStatus, MysqlRowEvent, MysqlSnapshotCursor,
};
use mysql_source::{ResolvedCredential, ResolvedMysqlEndpoint};
use serde_json::{Map, Number, Value};
use url::Url;

pub trait MysqlSourceDriver: Send + Sync {
    fn probe(
        &self,
        endpoint: &ResolvedMysqlEndpoint,
        credential: &ResolvedCredential,
    ) -> Result<MysqlProbeStatus>;

    fn discover_fields(
        &self,
        endpoint: &ResolvedMysqlEndpoint,
        credential: &ResolvedCredential,
    ) -> Result<MysqlFieldCatalog>;

    fn open_snapshot(
        &self,
        endpoint: &ResolvedMysqlEndpoint,
        credential: &ResolvedCredential,
        schema: &str,
        table: &str,
        fields: &[String],
    ) -> Result<Box<dyn MysqlSnapshotSession>>;
}

pub trait MysqlSnapshotSession: Send {
    fn fetch_page(&mut self, limit: usize, offset: u64) -> Result<MysqlSnapshotPage>;
    fn close(&mut self) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct MysqlSnapshotPage {
    pub fields: Vec<String>,
    pub rows: Vec<MysqlRowEvent>,
    pub next_cursor: Option<MysqlSnapshotCursor>,
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
        let mut conn = open_connection(endpoint, credential)?;
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
        let mut conn = open_connection(endpoint, credential)?;
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
        let mut conn = open_connection(endpoint, credential)?;
        conn.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .map_err(mysql_error("set mysql snapshot isolation"))?;
        conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            .map_err(mysql_error("start mysql snapshot transaction"))?;

        let selected_fields = if fields.is_empty() {
            load_table_fields(&mut conn, schema, table)?
        } else {
            fields.to_vec()
        };
        if selected_fields.is_empty() {
            return Err(CnxError::InvalidInput(format!(
                "mysql table `{schema}`.`{table}` has no selectable fields"
            )));
        }
        let order_by = endpoint
            .primary_key
            .as_deref()
            .filter(|field| selected_fields.iter().any(|candidate| candidate == field))
            .unwrap_or_else(|| selected_fields[0].as_str())
            .to_string();
        Ok(Box::new(NativeMysqlSnapshotSession {
            conn,
            schema: schema.to_string(),
            table: table.to_string(),
            fields: selected_fields,
            order_by,
            closed: false,
        }))
    }
}

struct NativeMysqlSnapshotSession {
    conn: Conn,
    schema: String,
    table: String,
    fields: Vec<String>,
    order_by: String,
    closed: bool,
}

impl MysqlSnapshotSession for NativeMysqlSnapshotSession {
    fn fetch_page(&mut self, limit: usize, offset: u64) -> Result<MysqlSnapshotPage> {
        let fetch_limit = limit.saturating_add(1);
        let field_list = self
            .fields
            .iter()
            .map(|field| quote_identifier(field))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT {field_list} FROM {}.{} ORDER BY {} LIMIT {} OFFSET {}",
            quote_identifier(&self.schema),
            quote_identifier(&self.table),
            quote_identifier(&self.order_by),
            fetch_limit,
            offset,
        );
        let mut rows: Vec<Row> = self
            .conn
            .query(sql)
            .map_err(mysql_error("fetch mysql snapshot page"))?;
        let has_more = rows.len() > limit;
        if has_more {
            rows.truncate(limit);
        }
        let events = rows
            .into_iter()
            .map(|row| row_to_event(&self.fields, row))
            .collect::<Result<Vec<_>>>()?;
        Ok(MysqlSnapshotPage {
            fields: self.fields.clone(),
            rows: events,
            next_cursor: None,
            has_more,
        })
    }

    fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.conn
            .query_drop("COMMIT")
            .or_else(|_| self.conn.query_drop("ROLLBACK"))
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
    if url
        .query_pairs()
        .any(|(key, value)| key.eq_ignore_ascii_case("ssl") && value.eq_ignore_ascii_case("true"))
    {
        builder = builder.ssl_opts(Some(SslOpts::default()));
    }
    Conn::new(Opts::from(builder)).map_err(mysql_error("connect mysql endpoint"))
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

fn row_to_event(fields: &[String], row: Row) -> Result<MysqlRowEvent> {
    let values = row.unwrap();
    let mut object = Map::with_capacity(fields.len());
    for (field, value) in fields.iter().zip(values) {
        object.insert(field.clone(), mysql_value_to_json(value)?);
    }
    Ok(MysqlRowEvent {
        values: Value::Object(object),
    })
}

fn mysql_value_to_json(value: MysqlValue) -> Result<Value> {
    Ok(match value {
        MysqlValue::NULL => Value::Null,
        MysqlValue::Bytes(bytes) => match String::from_utf8(bytes.clone()) {
            Ok(value) => Value::String(value),
            Err(_) => Value::String(base64_encode(&bytes)),
        },
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
    move |err| CnxError::PeerError(format!("{context}: {err}"))
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
