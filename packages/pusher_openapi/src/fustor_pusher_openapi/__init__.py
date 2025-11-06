"""
Fuagent pusher driver for OpenAPI endpoints. (Class-based refactoring)
"""
import httpx
import logging
from typing import List, Dict, Any, Tuple
from urllib.parse import urljoin

from fustor_core.drivers import PusherDriver
from fustor_core.exceptions import DriverError
from fustor_core.models.config import PusherConfig, PasswdCredential, ApiKeyCredential
from fustor_core.models.event import EventBase
from fustor_core.utils.retry import retry

logger = logging.getLogger("fustor_agent.driver.openapi")

class SessionObsoletedError(Exception):
    """当会话被标记为过时，不需要重试的异常"""
    pass

# Module-level cache for OpenAPI specifications to improve performance
_spec_cache = {}

class OpenApiDriver(PusherDriver):
    """
    A class-based driver for OpenAPI endpoints that conforms to the PusherDriver ABC.
    """

    def __init__(self, id: str, config: PusherConfig):
        """Initializes the driver with its specific configuration and a persistent HTTP client."""
        super().__init__(id, config)
        self.endpoint = self.config.endpoint
        self.credential = self.config.credential
        self.client = httpx.AsyncClient()

    async def close(self):
        """Gracefully closes the persistent HTTP client."""
        await self.client.aclose()

    @staticmethod
    async def _get_spec(client: httpx.AsyncClient, spec_url: str) -> Dict[str, Any]:
        """
        Static helper method to fetch and parse an OpenAPI specification.
        Uses the module-level cache.
        """
        if spec_url in _spec_cache:
            logger.debug(f"Using cached spec for {spec_url}")
            return _spec_cache[spec_url][0] # Return only the spec, not the post_endpoints

        logger.debug(f"Attempting to fetch OpenAPI spec from: {spec_url}")
        try:
            resp = await client.get(spec_url)
            resp.raise_for_status()
            spec = resp.json()
            logger.debug(f"Successfully fetched spec from {spec_url}.")
            # Cache the spec, even if post_endpoints are not extracted yet
            _spec_cache[spec_url] = (spec, []) # Store as (spec, post_endpoints)
            return spec
        except (httpx.RequestError, ValueError) as e:
            logger.error(f"Failed to get or parse OpenAPI schema from {spec_url}: {e}")
            raise DriverError(f"无法获取或解析位于 {spec_url} 的 OpenAPI 规范。")

    @staticmethod
    async def _get_all_post_endpoints_details(client: httpx.AsyncClient, spec_url: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Static helper method to parse a spec and extract details of all POST endpoints.
        Uses the module-level cache.
        """
        if spec_url in _spec_cache:
            logger.debug(f"Using cached spec for {spec_url}")
            return _spec_cache[spec_url]

        spec = await OpenApiDriver._get_spec(client, spec_url)
        post_endpoints = []
        for path, path_item in spec.get("paths", {}).items():
            if "post" in path_item:
                operation = path_item["post"]
                if "requestBody" in operation and "application/json" in operation["requestBody"]["content"]:
                    schema_ref_obj = operation["requestBody"]["content"]["application/json"]["schema"]
                    resolved_schema = schema_ref_obj
                    # Resolve $ref if present
                    if "$ref" in schema_ref_obj:
                        ref_path = schema_ref_obj["$ref"].split('/')
                        current_schema = spec
                        for part in ref_path[1:]:
                            current_schema = current_schema.get(part, {})
                        resolved_schema = current_schema
                    post_endpoints.append({"path": path, "schema": resolved_schema})
        
        if not post_endpoints:
            raise ValueError("No suitable POST endpoint with a requestBody found in the OpenAPI spec.")

        result = (spec, post_endpoints)
        _spec_cache[spec_url] = result
        logger.info(f"Fetched and cached spec details for {spec_url}")
        
        return result

    @retry(max_retries_attr='max_retries', delay_sec_attr='retry_delay_sec')
    async def get_latest_committed_index(self, **kwargs) -> int:
        """
        Implementation of the ABC method. Gets the last successfully processed index from the pusher.
        """
        session_id = kwargs.get("session_id")

        spec = await self._get_spec(self.client, self.endpoint)
        
        status_path = None
        servers = spec.get("servers", [])
        if servers and isinstance(servers, list) and len(servers) > 0:
            first_server = servers[0]
            if isinstance(first_server, dict):
                status_path = first_server.get("x-fustor_agent-status-endpoint")
        
        # If x-fustor_agent-status-endpoint is not defined in OpenAPI spec, extract the path from available endpoints
        if not status_path:
            # Look for endpoints that might be appropriate for status/checkpoint functionality
            paths = spec.get("paths", {})
            for path in paths:
                if "position" in path.lower():
                    # Check if this path supports GET and has parameters that include task_id or session_id
                    path_details = paths[path]
                    if "get" in path_details:
                        status_path = path
                        break
            
            # If still no appropriate path found, use a default
            if not status_path:
                logger.debug(f"Pusher endpoint {self.endpoint} spec does not define 'x-fustor_agent-status-endpoint' and no suitable status endpoint found in spec. Using default path '/checkpoint'.")
                status_path = "/checkpoint"
            else:
                logger.debug(f"Pusher endpoint {self.endpoint} spec does not define 'x-fustor_agent-status-endpoint', using discovered path '{status_path}' from available endpoints.")
            
            # For default path, we use the original endpoint URL as base
            status_url = urljoin(self.endpoint, status_path.lstrip('/'))
        else:
            server_url = spec.get("servers", [{}])[0].get("url", "")
            base_url = urljoin(self.endpoint, server_url)
            status_url = urljoin(base_url, status_path)

        headers = {}
        if hasattr(self.credential, 'to_base_64') and callable(self.credential.to_base_64):
            headers['Authorization'] = f"Basic {self.credential.to_base_64()}"
        elif hasattr(self.credential, 'key'):
            headers['Authorization'] = f"Bearer {self.credential.key}"
            headers['x-api-key'] = self.credential.key  # Ensure x-api-key header is present for generic compatibility
        
        try:
            params = {"session_id": session_id}
            logger.info(f"Querying latest index for task '{self.id}': GET {status_url}")
            resp = await self.client.get(status_url, headers=headers, params=params, timeout=10)
            
            if resp.status_code == 404:
                logger.warning(f"Checkpoint for task '{self.id}' not found (404). Will start from beginning.")
                return -1

            resp.raise_for_status()
            result = resp.json()
            
            index = result if isinstance(result, int) else result.get("index")
            if isinstance(index, int):
                logger.info(f"Got starting index for task '{self.id}': {index}")
                return index
            else:
                logger.error(f"Pusher status endpoint returned invalid format: {result}")
                raise DriverError("Pusher status endpoint returned invalid format.")

        except httpx.RequestError as e:
            logger.error(f"Network error while querying for index: {e}", exc_info=True)
            raise DriverError(f"Could not connect to pusher status endpoint: {status_url}")

    @retry(max_retries_attr='max_retries', delay_sec_attr='retry_delay_sec', exceptions=(DriverError,))
    async def push(self, events: List[EventBase], **kwargs) -> Dict:
        """
        Implementation of the ABC method. Pushes a batch of events to a single
        batch endpoint using an envelope schema.
        """
        session_id = kwargs.get("session_id")
        source_type = kwargs.get("source_type", "message")

        enhanced_events = []
        for event in events:
            event_type_value = event.event_type.value if hasattr(event, 'event_type') and event.event_type else 'unknown'
            for row in event.rows:
                enhanced_row = dict(row)
                enhanced_row["_event_type"] = event_type_value
                enhanced_events.append(enhanced_row)

        envelope = {
            "session_id": session_id,
            "events": enhanced_events,
            "source_type": source_type
        }

        logger.info(f"Pushing batch of {len(enhanced_events)} events for pusher '{self.id}' (session: {session_id or 'N/A'}).")

        spec, _ = await self._get_all_post_endpoints_details(self.client, self.endpoint)

        batch_endpoint_path = None
        servers = spec.get("servers", [])
        if servers and isinstance(servers, list) and len(servers) > 0:
            first_server = servers[0]
            if isinstance(first_server, dict):
                batch_endpoint_path = first_server.get("x-fustor_agent-ingest-batch-endpoint")        
        
        # If x-fustor_agent-ingest-batch-endpoint is not defined in OpenAPI spec, extract from available endpoints
        if not batch_endpoint_path:
            # Look for POST endpoints that might be appropriate for batch ingestion
            paths = spec.get("paths", {})
            for path in paths:
                path_details = paths[path]
                if "post" in path_details and "events" in path.lower():
                    # Check if this endpoint accepts batch events or ingestion payloads
                    operation = path_details["post"]
                    if "requestBody" in operation and "content" in operation["requestBody"]:
                        content_types = operation["requestBody"]["content"]
                        # Look for application/json request body
                        if "application/json" in content_types:
                            # This is likely a batch endpoint
                            batch_endpoint_path = path
                            break
        
            # If still no suitable path found, use a default
            if not batch_endpoint_path:
                logger.debug(f"Pusher endpoint {self.endpoint} spec does not define 'x-fustor_agent-ingest-batch-endpoint' and no suitable batch endpoint found in spec. Using default path '/batch'.")
                batch_endpoint_path = "/batch"
            else:
                logger.debug(f"Pusher endpoint {self.endpoint} spec does not define 'x-fustor_agent-ingest-batch-endpoint', using discovered path '{batch_endpoint_path}' from available endpoints.")
        
        # Construct the target URL regardless of how batch_endpoint_path was determined
        server_url_from_spec = spec.get("servers", [{}])[0].get("url", "")
        if server_url_from_spec and not server_url_from_spec.startswith('/'):
            base_url = urljoin(self.endpoint, server_url_from_spec)
        else:
            base_url = urljoin(self.endpoint, server_url_from_spec if server_url_from_spec else "/")
        target_url = urljoin(base_url, batch_endpoint_path.lstrip('/'))
            
        headers = {"Content-Type": "application/json"}
        
        if hasattr(self.credential, 'to_base_64') and callable(self.credential.to_base_64):
            headers['Authorization'] = f"Basic {self.credential.to_base_64()}"
        elif hasattr(self.credential, 'key'):
            headers['x-api-key'] = self.credential.key

        try:
            resp = await self.client.post(target_url, json=envelope, headers=headers, timeout=30.0)
            
            # Check for 419 status, which indicates the session is obsolete and should not be retried
            if resp.status_code == 419:
                logger.warning(f"Received 419 status from {target_url}, session is obsolete. Stopping without retry.")
                raise SessionObsoletedError(f"Session is obsolete, received 419 status from {target_url}")
            
            resp.raise_for_status()

            # Handle 204 No Content responses specially - they have no body and shouldn't be parsed as JSON
            if resp.status_code == 204:
                logger.debug(f"Pusher responded with 204 No Content, no response body to parse.")
                return {}

            try:
                response_data = resp.json()
                if not isinstance(response_data, dict):
                    logger.warning(f"Pusher response is not a JSON object: {response_data}")
                    return {}
                return response_data
            except ValueError:
                logger.warning(f"Pusher response was not valid JSON. Status: {resp.status_code}")
                return {}

        except httpx.HTTPStatusError as e:
            # Check if this is the 419 status code that was not caught earlier
            if e.response.status_code == 419:
                logger.warning(f"Received 419 status from {target_url}, session is obsolete. Stopping without retry.")
                raise SessionObsoletedError(f"Session is obsolete, received 419 status from {target_url}")
            
            logger.error(f"Failed to push batch to {target_url}. Status: {e.response.status_code}, Response: {e.response.text}")
            raise DriverError(f"HTTP Error {e.response.status_code} while pushing to pusher.")
        except httpx.RequestError as e:
            logger.error(f"Network error pushing batch to {target_url}: {e}")
            raise DriverError(f"Network error while pushing to pusher.")

    @retry(max_retries_attr='max_retries', delay_sec_attr='retry_delay_sec')
    async def heartbeat(self, **kwargs) -> Dict:
        """
        Sends a heartbeat to maintain session state with the pusher endpoint.
        The `kwargs` will contain `session_id`.
        """
        session_id = kwargs.get("session_id")
        
        if not session_id:
            raise DriverError("Session ID is required for heartbeat")
            
        # 从规范中获取心跳端点
        spec = await self._get_spec(self.client, self.endpoint)
        
        heartbeat_path = None
        servers = spec.get("servers", [])
        if servers and isinstance(servers, list) and len(servers) > 0:
            first_server = servers[0]
            if isinstance(first_server, dict):
                heartbeat_path = first_server.get("x-fustor_agent-heartbeat-endpoint")
        
        if not heartbeat_path:
            # 如果没有定义专门的心跳端点，尝试从可用端点中提取
            # Look for endpoints that might be appropriate for heartbeat functionality
            paths = spec.get("paths", {})
            for path in paths:
                if "heartbeat" in path.lower():
                    path_details = paths[path]
                    if "post" in path_details:
                        heartbeat_path = path
                        break
            
            # If still no heartbeat path found, look for session-related endpoints that might be used for heartbeats
            if not heartbeat_path:
                for path in paths:
                    if "session" in path.lower() and "heartbeat" in path.lower():
                        path_details = paths[path]
                        if "post" in path_details:
                            heartbeat_path = path
                            break
            
            # If still no suitable path found, use fallback
            if not heartbeat_path:
                logger.warning(f"Heartbeat endpoint not defined in spec for {self.endpoint} and no suitable heartbeat endpoint found in spec, using fallback.")
                
                # 尝试使用 /events/heartbeat 端点作为通用路径
                base_url = spec.get("servers", [{}])[0].get("url", "/")
                heartbeat_url = urljoin(self.endpoint, f"{base_url.rstrip('/')}/events/heartbeat")
            else:
                logger.debug(f"Pusher endpoint {self.endpoint} spec does not define 'x-fustor_agent-heartbeat-endpoint', using discovered path '{heartbeat_path}' from available endpoints.")
                server_url = spec.get("servers", [{}])[0].get("url", "")
                base_url = urljoin(self.endpoint, server_url)
                heartbeat_url = urljoin(base_url, heartbeat_path)
        else:
            server_url = spec.get("servers", [{}])[0].get("url", "")
            base_url = urljoin(self.endpoint, server_url)
            heartbeat_url = urljoin(base_url, heartbeat_path)
        
        headers = {"Content-Type": "application/json", "Session-ID": session_id}
        if hasattr(self.credential, 'to_base_64') and callable(self.credential.to_base_64):
            headers['Authorization'] = f"Basic {self.credential.to_base_64()}"
        elif hasattr(self.credential, 'key'):
            headers['Authorization'] = f"Bearer {self.credential.key}"
            headers['x-api-key'] = self.credential.key
        
        try:
            response = await self.client.post(
                heartbeat_url, 
                json={
                    "session_id": session_id,
                },
                headers=headers,
                timeout=5.0  # 心跳请求应该快速响应
            )
            
            # 检查状态码
            if response.status_code == 404:
                # 如果端点不存在，尝试其他方式或返回错误
                logger.warning(f"Heartbeat endpoint not found at {heartbeat_url}")
                return {"status": "error", "message": "heartbeat endpoint not found"}
            
            response.raise_for_status()
            return response.json() if response.content else {"status": "ok"}
        except httpx.HTTPStatusError as e:
            logger.warning(f"Heartbeat failed: {e.response.status_code}")
            raise DriverError(f"Heartbeat failed with status {e.response.status_code}")
        except httpx.RequestError as e:
            logger.warning(f"Network error during heartbeat: {e}")
            raise DriverError(f"Heartbeat network error: {e}")

    @retry(max_retries_attr='max_retries', delay_sec_attr='retry_delay_sec')
    async def create_session(self, task_id: str) -> Dict[str, Any]:
        """
        Creates a new session with the pusher endpoint.
        Returns the full session creation response dictionary.
        """
        
        # Get the OpenAPI spec to find the session endpoint
        spec = await self._get_spec(self.client, self.endpoint)
        
        # Look for the open session endpoint in the OpenAPI spec
        session_path = None
        servers = spec.get("servers", [])
        if servers and isinstance(servers, list) and len(servers) > 0:
            first_server = servers[0]
            if isinstance(first_server, dict):
                session_path = first_server.get("x-fustor_agent-open-session-endpoint")
        
        if not session_path:
            # Look for endpoints that might be appropriate for session creation
            paths = spec.get("paths", {})
            for path in paths:
                if "session" in path.lower():
                    path_details = paths[path]
                    if "post" in path_details:
                        session_path = path
                        break
            
            # If still no session path found, use a default
            if not session_path:
                logger.debug(f"Pusher endpoint {self.endpoint} spec does not define 'x-fustor_agent-open-session-endpoint' and no suitable session endpoint found in spec. Using default path '/events/session'.")
                session_path = "/events/session"
            else:
                logger.debug(f"Pusher endpoint {self.endpoint} spec does not define 'x-fustor_agent-open-session-endpoint', using discovered path '{session_path}' from available endpoints.")
        
        # Construct the session endpoint URL
        server_url = spec.get("servers", [{}])[0].get("url", "")
        base_url = urljoin(self.endpoint, server_url)
        session_url = urljoin(base_url, session_path)
        
        headers = {"Content-Type": "application/json"}
        if hasattr(self.credential, 'to_base_64') and callable(self.credential.to_base_64):
            headers['Authorization'] = f"Basic {self.credential.to_base_64()}"
        elif hasattr(self.credential, 'key'):
            headers['x-api-key'] = self.credential.key

        try:
            response = await self.client.post(
                session_url,
                json={
                    "task_id": task_id
                },
                headers=headers,
                timeout=10.0  # Session creation might take a bit longer
            )
            
            response.raise_for_status()
            
            # Parse the response to get the session ID
            response_data = response.json()
            if "session_id" in response_data:
                logger.info(f"Successfully created session {response_data['session_id']} for task: {task_id}")
                return response_data
            else:
                raise DriverError(f"Session creation response missing session_id: {response_data}")
                
        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to create session: {e.response.status_code}, Response: {e.response.text}")
            raise DriverError(f"HTTP Error {e.response.status_code} while creating session.")
        except httpx.RequestError as e:
            logger.error(f"Network error creating session: {e}")
            raise DriverError(f"Network error while creating session.")
    
    @classmethod
    async def get_needed_fields(cls, **kwargs) -> Dict[str, Any]:
        """
        Implementation of the ABC method. Declares the data fields required by this pusher.
        """
        endpoint = kwargs.get("endpoint")
        if not endpoint:
            raise DriverError("get_needed_fields requires 'endpoint' in arguments.")

        async with httpx.AsyncClient() as client:
            _spec, post_endpoints = await cls._get_all_post_endpoints_details(client, endpoint)
            
            merged_properties = {}
            merged_required = set()

            for ep in post_endpoints:
                schema = ep["schema"]
                name_segment = ep["path"].strip('/').split('/')[-1]
                
                if "properties" in schema:
                    # Helper to recursively extract fields and prefix them
                    def _extract_fields(properties, required_list, prefix=""):
                        props = {}
                        reqs = []
                        for name, definition in properties.items():
                            full_name = f"{prefix}{name}"
                            if definition.get("type") == "object" and "properties" in definition:
                                nested_props, nested_reqs = _extract_fields(definition["properties"], definition.get("required", []), prefix=f"{full_name}.")
                                props.update(nested_props)
                                reqs.extend(nested_reqs)
                            else:
                                props[full_name] = definition
                                if name in required_list:
                                    reqs.append(full_name)
                        return props, reqs

                    extracted_props, extracted_reqs = _extract_fields(schema["properties"], schema.get("required", []), prefix=f"{name_segment}.")
                    merged_properties.update(extracted_props)
                    merged_required.update(extracted_reqs)

            return {
                "type": "object",
                "properties": merged_properties,
                "required": list(merged_required)
            }

    @classmethod
    async def test_connection(cls, **kwargs) -> Tuple[bool, str]:
        """
        Implementation of the ABC method. Tests the connection to the source service.
        """
        endpoint = kwargs.get("endpoint")
        if not endpoint:
            return False, "Endpoint is required for connection test."
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.head(endpoint, timeout=5)
                resp.raise_for_status()
            return True, "成功连接到 OpenAPI 规范文件。"
        except httpx.HTTPStatusError as e:
            return False, f"连接失败: 服务器返回状态码 {e.response.status_code}。"
        except httpx.RequestError:
            return False, f"连接失败: 无法访问URL。请检查网络或URL地址。"

    @classmethod
    async def check_privileges(cls, **kwargs) -> Tuple[bool, str]:
        """
        Implementation of the ABC method. Checks if the provided credentials have sufficient privileges.
        """
        endpoint = kwargs.get("endpoint")
        credential_data = kwargs.get("credential")
        if not endpoint or not credential_data:
            return False, "Endpoint and credential are required for privilege check."

        # Re-create credential model from dict
        if 'key' in credential_data:
            credential = ApiKeyCredential(**credential_data)
        else:
            credential = PasswdCredential(**credential_data)

        headers = {"Content-Type": "application/json"}
        if isinstance(credential, PasswdCredential):
            headers['Authorization'] = f"Basic {credential.to_base_64()}"
        elif isinstance(credential, ApiKeyCredential):
            headers['Authorization'] = f"Bearer {credential.key}"
            headers['x-api-key'] = credential.key

        try:
            async with httpx.AsyncClient() as client:
                spec = await cls._get_spec(client, endpoint)
                
                status_path = None
                servers = spec.get("servers", [])
                if servers:
                    status_path = servers[0].get("x-fustor_agent-status-endpoint")

                # If x-fustor_agent-status-endpoint is not defined in OpenAPI spec, extract from available endpoints
                if not status_path:
                    # Look for endpoints that might be appropriate for status/checkpoint functionality
                    paths = spec.get("paths", {})
                    for path in paths:
                        if "position" in path.lower() or "checkpoint" in path.lower() or "status" in path.lower():
                            # Check if this path supports GET and has parameters that include task_id or session_id
                            path_details = paths[path]
                            if "get" in path_details:
                                status_path = path
                                break
                    
                    # If still no appropriate path found, return error
                    if not status_path:
                        return (False, "无法在 OpenAPI 规范中找到 'x-fustor_agent-status-endpoint' 或其他可用的状态端点，无法执行权限检查。")

                server_url = spec.get("servers", [{}])[0].get("url", "")
                base_url = urljoin(endpoint, server_url)
                status_url = urljoin(base_url, status_path)

                resp = await client.get(status_url, headers=headers, params={"task_id": "_privilege_check_"}, timeout=10)

                if resp.status_code in [401, 403]:
                    return False, f"凭证无效。测试请求 {status_url} 返回 {resp.status_code} (Unauthorized/Forbidden)。"
                
                return True, f"凭证有效。测试请求 {status_url} 成功通过认证 (状态码: {resp.status_code})。"

        except DriverError as e:
             return False, str(e)
        except httpx.RequestError as e:
            return False, f"网络错误，无法执行权限检查: {e}"
        except Exception as e:
            return False, f"发生意外错误: {e}"

    @classmethod
    def get_wizard_steps(cls) -> Dict[str, Any]:
        """Provides configuration wizard steps for UI integration."""
        return {
            "steps": [
                {
                    "step_id": "connection",
                    "title": "连接与凭证",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "endpoint": {
                                "type": "string",
                                "title": "OpenAPI Spec URL",
                                "description": "目标服务的 openapi.json 文件的完整URL。",
                                "format": "uri"
                            },
                            "credential": {
                                "type": "object",
                                "title": "凭证",
                                "description": "API 凭证 (支持 Basic Auth 或 API Key)",
                                "oneOf": [
                                    {"$ref": "#/components/schemas/PasswdCredential"},
                                    {"$ref": "#/components/schemas/ApiKeyCredential"}
                                ]
                            }
                        },
                        "required": ["endpoint", "credential"]
                    },
                    "validations": ["test_connection", "discover_fields", "check_privileges"]
                },
                {
                    "step_id": "advanced_settings",
                    "title": "高级参数",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "batch_size": {
                                "type": "integer",
                                "title": "批处理大小",
                                "description": "单次向接收端推送事件的最大数量。",
                                "default": 100
                            },
                            "max_retries": {
                                "type": "integer",
                                "title": "最大重试次数",
                                "description": "推送事件失败时的最大重试次数。",
                                "default": 10
                            },
                            "retry_delay_sec": {
                                "type": "integer",
                                "title": "重试延迟 (秒)",
                                "description": "每次重试前的等待秒数。",
                                "default": 5
                            }
                        }
                    },
                    "validations": []
                }
            ],
            "components": {
                "schemas": {
                    "PasswdCredential": {
                        "type": "object",
                        "title": "用户名/密码 (用于 Basic Auth)",
                        "properties": {
                            "user": {"type": "string", "title": "用户名"},
                            "passwd": {"type": "string", "title": "密码", "format": "password"}
                        },
                        "required": ["user"]
                    },
                    "ApiKeyCredential": {
                        "type": "object",
                        "title": "API Key",
                        "properties": {
                            "user": {"type": "string", "title": "用户 (可选)"},
                            "key": {"type": "string", "title": "API Key / Bearer Token", "format": "password"}
                        },
                        "required": ["key"]
                    }
                }
            }
        }
