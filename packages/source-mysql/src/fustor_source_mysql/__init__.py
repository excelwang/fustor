"Fuagent source driver for MySQL."
import time
import pymysql
from pymysql.cursors import SSCursor
import uuid
from typing import Iterator, Optional, Dict, Any, Tuple, List, Set
from decimal import Decimal
from datetime import datetime, date, timedelta
from contextlib import contextmanager
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent
import logging
import aiomysql
import threading
import json
import os

from fustor_core.drivers import SourceDriver
from fustor_core.models.config import SourceConfig, PasswdCredential
from fustor_core.exceptions import DriverError
from fustor_core.event import EventBase, InsertEvent, UpdateEvent, DeleteEvent

logger = logging.getLogger("fustor_agent.driver.mysql")

class MysqlDriver(SourceDriver):
    _instances: Dict[str, 'MysqlDriver'] = {}
    _lock = threading.Lock()
    
    def __new__(cls, id: str, config: SourceConfig):
        # Generate unique signature: URI + credential to ensure permission isolation
        signature = f"{config.uri}#{hash(str(config.credential))}"
        
        with MysqlDriver._lock:
            if signature not in MysqlDriver._instances:
                instance = super().__new__(cls)
                MysqlDriver._instances[signature] = instance
            return MysqlDriver._instances[signature]
    
    def __init__(self, id: str, config: SourceConfig):
        # Prevent re-initialization of shared instances
        if hasattr(self, '_initialized'):
            return
        
        super().__init__(id, config)
        self.uri = self.config.uri
        self.credential: PasswdCredential = self.config.credential
        self.column_maps: Dict[str, Dict[int, str]] = {}
        self._load_schema_and_build_map()
        
        self._initialized = True

    def _load_schema_and_build_map(self):
        schema_file_path = os.path.join('.conf', 'schemas', f'source_{self.id}.schema.json')
        if not os.path.exists(schema_file_path):
            logger.warning(f"Schema file not found for source '{self.id}' at '{schema_file_path}'. Binlog events will use placeholder column names.")
            return

        try:
            with open(schema_file_path, 'r', encoding='utf-8') as f:
                schema_data = json.load(f)
            
            for table_key, table_schema in schema_data.get("properties", {}).items():
                column_map: Dict[int, str] = {}
                for col_name, col_props in table_schema.get("properties", {}).items():
                    col_index = col_props.get("column_index")
                    if col_index is not None:
                        column_map[col_index] = col_name
                self.column_maps[table_key] = column_map
            logger.info(f"Successfully loaded schema and built column maps for source '{self.id}'.")
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Failed to load or parse schema file '{schema_file_path}': {e}", exc_info=True)

    def _get_row_with_column_names(self, table_key: str, values: List[Any]) -> Dict[str, Any]:
        column_map = self.column_maps.get(table_key)
        if not column_map:
            return {f"UNKNOWN_COL{i}": val for i, val in enumerate(values)}
        
        row_dict = {}
        for i, val in enumerate(values):
            col_name = column_map.get(i, f"UNKNOWN_COL{i}")
            row_dict[col_name] = val
        return row_dict

    def get_snapshot_iterator(self, **kwargs) -> Iterator[EventBase]:
        stream_id = f"snapshot-{uuid.uuid4().hex[:6]}"
        logger.info(f"[{stream_id}] Starting Consistent Snapshot.")
        
        snapshot_conn = None
        try:
            host, port_str = self.uri.split(':')
            snapshot_conn = pymysql.connect(
                host=host, port=int(port_str), user=self.credential.user, passwd=self.credential.passwd or ''
            )
            
            with snapshot_conn.cursor(SSCursor) as cursor:
                cursor.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
                logger.info(f"[{stream_id}] Transaction started for consistent snapshot.")
                
                cursor.execute("SHOW MASTER STATUS")
                status = cursor.fetchone()
                if not status:
                    raise DriverError("Could not get master status to determine snapshot position.")
                
                binlog_start_pos_int = _generate_event_index(status[0], status[1])
                logger.info(f"[{stream_id}] Consistent snapshot locked at position: {binlog_start_pos_int} ({status[0]}:{status[1]})")

                required_fields = kwargs.get("required_fields_tracker").get_fields() if kwargs.get("required_fields_tracker") else set()
                table_columns: Dict[Tuple[str, str], List[str]] = {}
                for full_field_name in required_fields:
                    field_parts = full_field_name.split('.')
                    if len(field_parts) < 3: continue
                    schema, table_name, column_name = field_parts[0], field_parts[1], field_parts[2]
                    key = (schema, table_name)
                    if key not in table_columns:
                        table_columns[key] = []
                    table_columns[key].append(column_name)

                for (schema, table_name), columns in table_columns.items():
                    if not columns: continue
                    columns_csv = ', '.join([f"`{col}`" for col in columns])
                    query = f"SELECT {columns_csv} FROM `{schema}`.`{table_name}`"
                    
                    logger.debug(f"[{stream_id}] Executing snapshot query: {query}")
                    cursor.execute(query)
                    
                    batch_size = kwargs.get("batch_size", 100)
                    while True:
                        batch = cursor.fetchmany(batch_size)
                        if not batch:
                            break
                        
                        rows = [{columns[i]: _normalize_row(col) for i, col in enumerate(row)} for row in batch]
                        if rows:
                            event = InsertEvent(event_schema, table_name, rows, index=binlog_start_pos_int)
                            yield event
                
                snapshot_conn.commit()
                logger.info(f"[{stream_id}] Snapshot transaction committed.")

        except Exception as e:
            if snapshot_conn:
                snapshot_conn.rollback()
            logger.error(f"[{stream_id}] Snapshot sync phase failed, transaction rolled back: {e}", exc_info=True)
            raise
        finally:
            if snapshot_conn:
                snapshot_conn.close()

    def is_position_available(self, position: int) -> bool:
        """
        Checks if the MySQL binlog position is available for resuming.
        """
        if position <= 0: #means from the latest snapshot
            return False
            
        try:
            logger.debug(f"Checking availability of binlog position {position}")
            with _create_binlog_streamer(self.uri, self.credential, position, "pos-check", None, connect_timeout=5) as checker:
                pass # If context manager succeeds, position is valid
            logger.debug(f"Binlog position {position} is available.")
            return True
        except Exception as e:
            # Broad exception to catch various pymysqlreplication errors for lost logs
            logger.warning(f"Binlog position {position} is not available (Reason: {e}).")
            return False

    def get_message_iterator(self, start_position: int=-1, **kwargs) -> Iterator[EventBase]:
        """
        Performs incremental data capture (CDC).
        """

        def _iterator_func() -> Iterator[EventBase]:
            stream_id = f"message-stream-{uuid.uuid4().hex[:6]}"
           
            stop_event = kwargs.get("stop_event")
            required_fields_tracker = kwargs.get("required_fields_tracker")
            max_retries = self.config.max_retries
            retry_delay_sec = self.config.retry_delay_sec

            event_id_from = start_position if start_position != -1 else 0
            attempt = 0
            while attempt < max_retries:
                if stop_event and stop_event.is_set(): break
                try:
                    with _create_binlog_streamer(self.uri, self.credential, event_id_from, stream_id, stop_event) as streamer:
                        for binlog_event in streamer:
                            if stop_event and stop_event.is_set(): break
                            
                            if required_fields_tracker and required_fields_tracker.wait_for_change(timeout=0.1):
                                required_fields_tracker.clear_event()

                            if streamer.log_file is None or streamer.log_pos is None: continue
                            event_index = _generate_event_index(streamer.log_file, streamer.log_pos)
                            event = None
                            if hasattr(binlog_event, 'rows') and binlog_event.rows:
                                table_key = f"{binlog_event.event_schema}.{binlog_event.table}"
                                if isinstance(binlog_event, WriteRowsEvent):
                                    rows = [_normalize_row(self._get_row_with_column_names(table_key, row['values'])) for row in binlog_event.rows]
                                    event = InsertEvent(binlog_event.event_schema, binlog_event.table, rows, index=event_index)
                                elif isinstance(binlog_event, UpdateRowsEvent):
                                    rows = [_normalize_row(self._get_row_with_column_names(table_key, row['after_values'])) for row in binlog_event.rows]
                                    event = UpdateEvent(binlog_event.event_schema, binlog_event.table, rows, index=event_index)
                                elif isinstance(binlog_event, DeleteRowsEvent):
                                    rows = [_normalize_row(self._get_row_with_column_names(table_key, row['values'])) for row in binlog_event.rows]
                                    event = DeleteEvent(binlog_event.event_schema, binlog_event.table, rows, index=event_index)
                            
                            if event:
                                filtered_event = _filter_event_rows(event, required_fields_tracker.get_fields() if required_fields_tracker else set())
                                if filtered_event:
                                    yield filtered_event
                            
                            event_id_from = event_index
                        
                        if stop_event and stop_event.is_set(): break
                    break
                except Exception as e:
                    attempt += 1
                    if attempt < max_retries:
                        logger.warning(f"[{stream_id}] Transient error in binlog stream (attempt {attempt}/{max_retries}): {e}")
                        time.sleep(retry_delay_sec)
                    else:
                        logger.error(f"[{stream_id}] Failed after {max_retries} retries in binlog stream: {e}", exc_info=True)
                        raise DriverError(f"Binlog streaming failed after {max_retries} retries: {e}")

            logger.info(f"[{stream_id}] Message iterator finished.")

        return _iterator_func()

    @classmethod
    async def test_connection(cls, **kwargs) -> Tuple[bool, str]:
        uri = kwargs.get("uri")
        admin_creds_dict = kwargs.get("admin_creds", {})
        if not uri or not admin_creds_dict:
            return (False, "缺少 'uri' 或 'admin_creds' 参数")
        creds = PasswdCredential(**admin_creds_dict)
        
        conn = None
        try:
            conn = await _get_connection(uri, creds)
            logger.info(f"Successfully tested connection to {uri}")
            return True, "数据库连接成功。"
        except Exception as e:
            logger.error(f"MySQL async test_connection failed: {e}", exc_info=True)
            return False, f"数据库连接失败: {e}"
        finally:
            if conn is not None:
                close = getattr(conn, "close", None)
                if callable(close):
                    close()

    @classmethod
    async def check_runtime_params(cls, **kwargs) -> Tuple[bool, str]:
        uri = kwargs.get("uri")
        admin_creds_dict = kwargs.get("admin_creds", {})
        if not uri or not admin_creds_dict:
            return (False, "缺少 'uri' 或 'admin_creds' 参数")
        admin_creds = PasswdCredential(**admin_creds_dict)

        conn = None
        try:
            conn = await _get_connection(uri, admin_creds)
            async with conn.cursor() as cursor:
                await cursor.execute("SHOW GLOBAL VARIABLES LIKE 'log_bin'")
                log_bin = await cursor.fetchone()
                if not log_bin or log_bin[1] != 'ON':
                    return (False, "配置检查失败: 全局变量 'log_bin' 必须为 'ON'")
                
                await cursor.execute("SHOW GLOBAL VARIABLES LIKE 'binlog_format'")
                binlog_format = await cursor.fetchone()
                if not binlog_format or binlog_format[1] != 'ROW':
                    return (False, "配置检查失败: 全局变量 'binlog_format' 必须为 'ROW'")
            logger.info("Runtime parameters check passed")
            return True, "运行时参数有效。"
        except Exception as e:
            logger.error(f"MySQL check_runtime_params failed: {e}", exc_info=True)
            return False, f"检查运行时参数失败: {e}"
        finally:
            if conn is not None:
                close = getattr(conn, "close", None)
                if callable(close):
                    close()

    @classmethod
    async def create_agent_user(cls, **kwargs) -> Tuple[bool, str]:
        uri = kwargs.get("uri")
        admin_creds_dict = kwargs.get("admin_creds", {})
        agent_user_dict = kwargs.get("credential", {})
        if not uri or not admin_creds_dict or not agent_user_dict:
            return (False, "缺少 'uri', 'admin_creds', 或 'credential' 参数")
        admin_creds = PasswdCredential(**admin_creds_dict)
        agent_user = PasswdCredential(**agent_user_dict)
        
        conn = None
        try:
            conn = await _get_connection(uri, admin_creds)
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "CREATE USER IF NOT EXISTS %s@%s IDENTIFIED BY %s",
                    (agent_user.user, '%', agent_user.passwd or '')
                )
                await cursor.execute(
                    "GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO %s@%s",
                    (agent_user.user, '%')
                )
                await cursor.execute("FLUSH PRIVILEGES")
            logger.info(f"User '{agent_user.user}' is ready for replication.")
            return True, f"用户 '{agent_user.user}' 已成功创建或验证。"
        except Exception as e:
            logger.error(f"Failed to create or grant privileges to user '{agent_user.user}': {e}", exc_info=True)
            return False, f"创建或授权用户 '{agent_user.user}' 失败: {e}"
        finally:
            if conn is not None:
                close = getattr(conn, "close", None)
                if callable(close):
                    close()

    @classmethod
    async def check_privileges(cls, **kwargs) -> Tuple[bool, str]:
        uri = kwargs.get("uri")
        admin_creds_dict = kwargs.get("admin_creds", {})
        agent_user_dict = kwargs.get("credential", {})
        if not uri or not admin_creds_dict or not agent_user_dict:
            return (False, "缺少 'uri', 'admin_creds', 或 'credential' 参数")
        admin_creds = PasswdCredential(**admin_creds_dict)
        agent_user = PasswdCredential(**agent_user_dict)
        
        conn = None
        try:
            conn = await _get_connection(uri, admin_creds)
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT Repl_slave_priv, Repl_client_priv, Select_priv FROM mysql.user WHERE User = %s AND Host = %s",
                    (agent_user.user, '%')
                )
                result = await cursor.fetchone()
                if not result or result[0] != 'Y' or result[1] != 'Y' or result[2] != 'Y':
                    msg = f"用户 '{agent_user.user}' 缺少必要的权限 (REPLICATION SLAVE, REPLICATION CLIENT, SELECT)。"
                    logger.error(msg)
                    return False, msg

            logger.info(f"User '{agent_user.user}' privileges verified")
            return True, f"用户 '{agent_user.user}' 权限充足。"
        except Exception as e:
            logger.error(f"MySQL check_user_privileges failed for user '{agent_user.user}': {e}", exc_info=True)
            return False, f"检查用户 '{agent_user.user}' 权限失败: {e}"
        finally:
            if conn is not None:
                close = getattr(conn, "close", None)
                if callable(close):
                    close()

    @classmethod
    async def get_available_fields(cls, **kwargs) -> Dict[str, Any]:
        uri = kwargs.get("uri")
        admin_creds_dict = kwargs.get("admin_creds")
        if not uri or not admin_creds_dict:
            raise DriverError("get_available_fields requires 'uri' and 'admin_creds'.")
        
        creds = PasswdCredential(**admin_creds_dict)

        conn = None
        try:
            conn = await _get_connection(uri, creds)
            available_fields = {}
            system_schemas = ('information_schema', 'mysql', 'performance_schema', 'sys')
            
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA NOT IN %s ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION", (system_schemas,))
                rows = await cursor.fetchall()
                for row in rows:
                    composite_key = f"{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}.{row['COLUMN_NAME']}"
                    available_fields[composite_key] = {"type": "string", "column_index": row['ORDINAL_POSITION'] - 1} 
            
            logger.info(f"Successfully retrieved {len(available_fields)} available fields from {uri}.")
            return {"properties": available_fields}
        except pymysql.err.OperationalError as e:
            error_message = f"连接到 MySQL 失败: 访问被拒绝。请检查用户名、密码和主机权限。"
            logger.debug(f"Original MySQL connection error in mysql driver: {e}", exc_info=True)
            raise DriverError(error_message) from e
        except Exception as e:
            logger.error(f"Error getting available fields: {e}", exc_info=True)
            raise
        finally:
            if conn is not None:
                conn.close()
    

# --- Module-level helper functions and classes ---

@contextmanager
def _create_binlog_streamer(
   uri: str, user_creds: PasswdCredential, event_id_from: int, stream_id: str, stop_event: Optional[threading.Event] = None, connect_timeout: int = 30
) -> Iterator[BinLogStreamReader]:
    streamer = None
    try:
        host, port_str = uri.split(':')
        mysql_settings = {
            "host": host,
            "port": int(port_str),
            "user": user_creds.user,
            "passwd": user_creds.passwd or ''
        }
        
        log_file, log_pos = _parse_event_index(event_id_from)

        server_id = 10086 + int(uuid.uuid4().hex[:8], 16) % 1000
        streamer = BinLogStreamReader(
            connection_settings=mysql_settings,
            server_id=server_id,
            resume_stream=True,
            log_file=log_file,
            log_pos=log_pos,
            blocking=True,
            only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent]
        )
        logger.info(f"Stream {stream_id}: Started MySQL binlog monitoring from {log_file}:{log_pos} with server_id {server_id}")
        if stop_event and stop_event.is_set():
            logger.info(f"Stream {stream_id}: Stop event already set, not starting binlog stream.")
            return
        yield streamer
    except Exception as e:
        logger.error(f"Stream {stream_id}: Failed to create BinLogStreamReader: {e}", exc_info=True)
        raise
    finally:
        if streamer:
            streamer.close()
            logger.info(f"Stream {stream_id}: MySQL binlog stream closed.")

async def _get_connection(uri: str, creds: PasswdCredential) -> aiomysql.Connection:
    host, port_str = uri.split(':')
    return await aiomysql.connect(
        host=host, port=int(port_str), user=creds.user,
        password=creds.passwd or '', autocommit=True
    )

def _generate_event_index(log_file: str, log_pos: int) -> int:
    if not log_file:
        return 0
    try:
        return (int(log_file.split('.')[-1]) << 32) | log_pos
    except (ValueError, IndexError):
        logger.warning(f"Invalid log_file format: {log_file}, returning default index 0")
        return 0

def _parse_event_index(index: int) -> Tuple[Optional[str], int]:
    if index == 0:
        return None, 4
    try:
        return f"mysql-bin.{index >> 32:06d}", index & 0xFFFFFFFF
    except Exception as e:
        logger.error(f"Failed to parse event index {index}: {e}", exc_info=True)
        return None, 4

def _normalize_row(data):
    if isinstance(data, dict):
        return {k: _normalize_row(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_normalize_row(item) for item in data]
    if isinstance(data, (datetime, date, timedelta)):
        return str(data)
    if isinstance(data, Decimal):
        return float(data)
    return data

def _filter_event_rows(event: EventBase, required_fields: Set[str]) -> Optional[EventBase]:
    if not required_fields:
        return event
    
    event_prefix = f"{event.event_schema}.{event.table}."
    if not any(f.startswith(event_prefix) for f in required_fields):
        return None

    filtered_rows = []
    for row in event.rows:
        filtered_row = {}
        for field_name, field_value in row.items():
            full_field_name = f"{event.event_schema}.{event.table}.{field_name}"
            if full_field_name in required_fields:
                filtered_row[field_name] = field_value
        
        if filtered_row:
            filtered_rows.append(filtered_row)
    
    if filtered_rows:
        new_event = type(event)(event.event_schema, event.table, filtered_rows)
        new_event.fields = list(filtered_rows[0].keys())
        new_event.index = event.index
        return new_event
        
    return None