"""
Fuagent source driver for Elasticsearch.
"""
import logging
import threading
from typing import Any, Dict, Iterator, Tuple
from datetime import datetime

from elasticsearch import Elasticsearch, AsyncElasticsearch, AuthenticationException, AuthorizationException

from fustor_core.drivers import SourceDriver
from fustor_core.models.config import SourceConfig, PasswdCredential, ApiKeyCredential
from fustor_core.exceptions import DriverError
from fustor_core.event import EventBase, InsertEvent

logger = logging.getLogger("fustor_agent.driver.elasticsearch")

class ElasticsearchDriver(SourceDriver):
    _instances: Dict[str, 'ElasticsearchDriver'] = {}
    _lock = threading.Lock()
    
    def __new__(cls, id: str, config: SourceConfig):
        # Generate unique signature: URI + credential to ensure permission isolation
        signature = f"{config.uri}#{hash(str(config.credential))}"
        
        with ElasticsearchDriver._lock:
            if signature not in ElasticsearchDriver._instances:
                instance = super().__new__(cls)
                ElasticsearchDriver._instances[signature] = instance
            return ElasticsearchDriver._instances[signature]
    
    def __init__(self, id: str, config: SourceConfig):
        # Prevent re-initialization of shared instances
        if hasattr(self, '_initialized'):
            return
        
        super().__init__(id, config)
        self.uri = self.config.uri
        self.credential = self.config.credential
        self.driver_params = self.config.driver_params
        
        self._initialized = True

    def _get_es_client(self) -> Elasticsearch:
        return self._get_sync_es_client(self.uri, self.credential)

    @staticmethod
    def _get_sync_es_client(uri: str, credential: Any) -> Elasticsearch:
        auth_params = {}
        if isinstance(credential, PasswdCredential) and credential.user:
            auth_params = (credential.user, credential.passwd or '')
        elif isinstance(credential, ApiKeyCredential) and credential.key:
            auth_params = {'api_key': credential.key}
        
        return Elasticsearch(
            hosts=[uri], 
            basic_auth=auth_params if isinstance(auth_params, tuple) else None, 
            api_key=auth_params.get('api_key') if isinstance(auth_params, dict) else None
        )

    @staticmethod
    async def _get_async_es_client(uri: str, credential: Any) -> AsyncElasticsearch:
        auth_params = {}
        if isinstance(credential, PasswdCredential) and credential.user:
            auth_params = (credential.user, credential.passwd or '')
        elif isinstance(credential, ApiKeyCredential) and credential.key:
            auth_params = {'api_key': credential.key}

        return AsyncElasticsearch(
            hosts=[uri],
            basic_auth=auth_params if isinstance(auth_params, tuple) else None,
            api_key=auth_params.get('api_key') if isinstance(auth_params, dict) else None
        )

    def get_snapshot_iterator(self, **kwargs) -> Iterator[EventBase]:
        index_name = self.driver_params.get("index_name")
        timestamp_field = self.driver_params.get("timestamp_field")
        if not index_name or not timestamp_field:
            raise DriverError("'index_name' and 'timestamp_field' are required driver parameters.")

        client = self._get_es_client()
        logger.info(f"Starting snapshot for Elasticsearch index '{index_name}'.")
        
        pit = client.open_point_in_time(index=index_name, keep_alive="1m")
        try:
            search_after = None
            snapshot_time = int(datetime.now().timestamp() * 1000)
            while True:
                resp = client.search(
                    index=index_name, 
                    size=kwargs.get("batch_size", 100), 
                    sort=[{timestamp_field: "asc"}, {"_doc": "asc"}], 
                    pit={"id": pit['id'], "keep_alive": "1m"}, 
                    search_after=search_after
                )
                hits = resp['hits']['hits']
                if not hits:
                    break
                
                rows = [_normalize_doc(h) for h in hits]
                fields = list(rows[0].keys()) if rows else []
                yield InsertEvent(event_schema=index_name, table=index_name, rows=rows, fields=fields, index=snapshot_time)
                
                search_after = hits[-1]['sort']
        finally:
            if pit:
                client.close_point_in_time(id=pit['id'])
            logger.info(f"Snapshot for Elasticsearch index '{index_name}' finished.")

    def is_position_available(self, position: int) -> bool:
        """
        Checks if the Elasticsearch position (timestamp) is available for resuming.
        Since Elasticsearch doesn't have the same concept of positions as databases,
        we generally consider positions available but may need to check if they're too old.
        """
        # For now, assume all positions are available as ES allows timestamp-based queries
        # TODO In the future, we might check if the position is older than retention period
        return True

    def get_message_iterator(self, start_position: int=-1, **kwargs) -> Iterator[EventBase]:
        
        def _iterator_func() -> Iterator[EventBase]:
            index_name = self.driver_params.get("index_name")
            timestamp_field = self.driver_params.get("timestamp_field")
            polling_interval = self.driver_params.get("polling_interval_sec", 5)
            if not index_name or not timestamp_field:
                raise DriverError("'index_name' and 'timestamp_field' are required driver parameters.")

            client = self._get_es_client()
            start_timestamp = start_position if start_position!=-1 else int(datetime.now().timestamp() * 1000)
            last_ts_iso = datetime.fromtimestamp(start_timestamp / 1000).isoformat()
            stop_event = kwargs.get("stop_event")

            while not stop_event.is_set():
                resp = client.search(
                    index=index_name, 
                    query={"range": {timestamp_field: {"gt": last_ts_iso}}},
                    sort=[{timestamp_field: "asc"}], 
                    size=100
                )
                hits = resp['hits']['hits']
                if not hits:
                    stop_event.wait(timeout=polling_interval)
                    continue

                for hit in hits:
                    if stop_event.is_set():
                        break
                    doc = _normalize_doc(hit)
                    ts_str = doc.get(timestamp_field)
                    if ts_str:
                        dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                        current_ts_ms = int(dt.timestamp() * 1000)
                        yield UpdateEvent(event_schema=index_name, table=index_name, rows=[doc], fields=list(doc.keys()), index=current_ts_ms)
                        last_ts_iso = dt.isoformat()
        
        return _iterator_func()

    @classmethod
    async def get_available_fields(cls, **kwargs) -> Dict[str, Any]:
        uri = kwargs.get("uri")
        credential_data = kwargs.get("credential")
        driver_params = kwargs.get("driver_params", {})
        index_name = driver_params.get("index_name")

        if uri is None or credential_data is None or index_name is None:
            raise DriverError("'uri', 'credential', and 'driver_params.index_name' are required.")

        client = await cls._get_async_es_client(uri, credential_data)
        try:
            mapping = await client.indices.get_mapping(index=index_name)
            properties = mapping[index_name]["mappings"].get("properties", {})
            
            flat_properties = {}
            def flatten(props, prefix=''):
                for key, value in props.items():
                    if "properties" in value:
                        flatten(value["properties"], f"{prefix}{key}.")
                    else:
                        flat_properties[f"{prefix}{key}"] = {"type": value.get("type", "object")}
            
            flatten(properties)
            flat_properties["_id"] = {"type": "keyword"}
            flat_properties["_index"] = {"type": "keyword"}

            return {"properties": flat_properties}
        except Exception as e:
            logger.error(f"Failed to get available fields for ES index '{index_name}': {e}")
            raise DriverError(f"Failed to get mapping for index '{index_name}': {e}")
        finally:
            await client.close()

    @classmethod
    async def test_connection(cls, **kwargs) -> Tuple[bool, str]:
        uri = kwargs.get("uri")
        credential_data = kwargs.get("credential")
        if uri is None or credential_data is None:
            return False, "'uri' and 'credential' are required."

        try:
            client = await cls._get_async_es_client(uri, credential_data)
            if await client.ping():
                return True, "Successfully connected to Elasticsearch."
            else:
                return False, "Connection to Elasticsearch failed."
        except (AuthenticationException, AuthorizationException) as e:
            return False, f"Authentication/Authorization failed: {e}"
        except Exception as e:
            return False, f"An unexpected error occurred: {e}"
        finally:
            if 'client' in locals():
                await client.close()

    @classmethod
    async def check_privileges(cls, **kwargs) -> Tuple[bool, str]:
        uri = kwargs.get("uri")
        credential_data = kwargs.get("credential")
        driver_params = kwargs.get("driver_params", {})
        index_name = driver_params.get("index_name")

        if uri is None or credential_data is None or index_name is None:
            return False, "'uri', 'credential', and 'driver_params.index_name' are required."

        client = await cls._get_async_es_client(uri, credential_data)
        try:
            response = await client.security.has_privileges(
                body={"index": [{"names": [index_name], "privileges": ["read"]}]}
            )
            if response.get("has_all_requested"):
                return True, "User has sufficient privileges for the index."
            else:
                return False, f"User lacks 'read' privilege for index '{index_name}'."
        except Exception as e:
            logger.warning(f"Could not verify privileges via security API (may not be enabled): {e}. Assuming success.")
            return True, "Could not verify privileges via security API; assuming success."
        finally:
            await client.close()

    @classmethod
    async def get_wizard_steps(cls) -> Dict[str, Any]:
        return {
            "steps": [
                {
                    "step_id": "connection",
                    "title": "Connection Details",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "uri": {"type": "string", "title": "Elasticsearch URI"},
                            "credential": {
                                "type": "object",
                                "title": "Credentials",
                                "oneOf": [
                                    {"$ref": "#/components/schemas/PasswdCredential"},
                                    {"$ref": "#/components/schemas/ApiKeyCredential"}
                                ]
                            }
                        },
                        "required": ["uri", "credential"]
                    },
                    "validations": ["test_connection"]
                },
                {
                    "step_id": "index_config",
                    "title": "Index Configuration",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "driver_params": {
                                "type": "object",
                                "title": "Driver Parameters",
                                "properties": {
                                    "index_name": {"type": "string", "title": "Index Name"},
                                    "timestamp_field": {"type": "string", "title": "Timestamp Field"}
                                },
                                "required": ["index_name", "timestamp_field"]
                            }
                        },
                        "required": ["driver_params"]
                    },
                    "validations": ["check_privileges", "discover_fields_no_cache"]
                }
            ],
            "components": {
                "schemas": {
                    "PasswdCredential": {
                        "type": "object",
                        "title": "Username/Password",
                        "properties": {
                            "user": {"type": "string", "title": "Username"},
                            "passwd": {"type": "string", "title": "Password", "format": "password"}
                        },
                        "required": ["user"]
                    },
                    "ApiKeyCredential": {
                        "type": "object",
                        "title": "API Key",
                        "properties": {
                            "key": {"type": "string", "title": "API Key", "format": "password"}
                        },
                        "required": ["key"]
                    }
                }
            }
        }

def _normalize_doc(hit: Dict[str, Any]) -> Dict[str, Any]:
    doc = hit.get('_source', {})
    doc['_id'] = hit.get('_id')
    doc['_index'] = hit.get('_index')
    return doc