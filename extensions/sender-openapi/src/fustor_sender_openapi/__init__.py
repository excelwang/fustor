"""
Fuagent sender driver for OpenAPI endpoints. (Class-based refactoring)
"""
import httpx
import logging
from typing import List, Dict, Any, Tuple, Optional
from urllib.parse import urljoin

from fustor_core.transport import Sender
from fustor_core.exceptions import DriverError, SessionObsoletedError
from fustor_core.models.config import SenderConfig, ApiKeyCredential, PasswdCredential
from fustor_core.event import EventBase
from fustor_core.utils.retry import retry

logger = logging.getLogger("fustor_agent.driver.openapi")


# Module-level cache for OpenAPI specifications to improve performance
_spec_cache = {}

class OpenApiDriver(Sender):
    """
    A class-based driver for OpenAPI endpoints that conforms to the Sender ABC.
    """

    def __init__(self, sender_id: str, endpoint: str, credential: Dict[str, Any], config: Optional[Dict[str, Any]] = None):
        """Initializes the driver with its specific configuration and a persistent HTTP client."""
        super().__init__(sender_id, endpoint, credential, config)
        self.client = httpx.AsyncClient()

    async def _send_events_impl(
        self, 
        events: List[EventBase], 
        source_type: str = "message", 
        is_end: bool = False, 
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Dict:
        """
        Implementation of OpenAPI sending.
        """
        session_id = self.session_id

        envelope = {
            "session_id": session_id,
            "events": [event.model_dump(mode='json') for event in events], # Send EventBase objects as dicts
            "source_type": source_type
        }

        logger.info(f"Sending batch of {len(events)} events for sender '{self.id}' (session: {session_id or 'N/A'}, phase: {source_type}).")


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
                logger.debug(f"Sender endpoint {self.endpoint} spec does not define 'x-fustor_agent-ingest-batch-endpoint' and no suitable batch endpoint found in spec. Using default path '/batch'.")
                batch_endpoint_path = "/batch"
            else:
                logger.debug(f"Sender endpoint {self.endpoint} spec does not define 'x-fustor_agent-ingest-batch-endpoint', using discovered path '{batch_endpoint_path}' from available endpoints.")
        
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
                logger.debug(f"Sender responded with 204 No Content, no response body to parse.")
                return {}

            try:
                response_data = resp.json()
                if not isinstance(response_data, dict):
                    logger.warning(f"Sender response is not a JSON object: {response_data}")
                    return {}
                return response_data
            except ValueError:
                logger.warning(f"Sender response was not valid JSON. Status: {resp.status_code}")
                return {}

        except httpx.HTTPStatusError as e:
            # Check if this is the 419 status code that was not caught earlier
            if e.response.status_code == 419:
                logger.warning(f"Received 419 status from {target_url}, session is obsolete. Stopping without retry.")
                raise SessionObsoletedError(f"Session is obsolete, received 419 status from {target_url}")
            
            logger.error(f"Failed to push batch to {target_url}. Status: {e.response.status_code}, Response: {e.response.text}")
            raise DriverError(f"HTTP Error {e.response.status_code} while pushing to sender.")
        except httpx.RequestError as e:
            logger.error(f"Network error pushing batch to {target_url}: {e}")
            raise DriverError(f"Network error while pushing to sender.")

    async def connect(self) -> None:
        """Establish connection (for HTTP, this is a no-op as we use stateless requests)."""
        logger.debug(f"OpenAPI Sender {self.id} ready.")

    @retry(max_retries_attr='max_retries', delay_sec_attr='retry_delay_sec')
    async def create_session(
        self, 
        task_id: str, 
        source_type: Optional[str] = None,
        session_timeout_seconds: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Creates a new session with the sender endpoint.
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
                logger.debug(f"Sender endpoint {self.endpoint} spec does not define 'x-fustor_agent-open-session-endpoint' and no suitable session endpoint found in spec. Using default path '/events/session'.")
                session_path = "/events/session"
            else:
                logger.debug(f"Sender endpoint {self.endpoint} spec does not define 'x-fustor_agent-open-session-endpoint', using discovered path '{session_path}' from available endpoints.")
        
        # Construct the session endpoint URL
        server_url = spec.get("servers", [{}])[0].get("url", "")
        base_url = urljoin(self.endpoint, server_url)
        session_url = urljoin(base_url, session_path)
        
        headers = {"Content-Type": "application/json"}
        if hasattr(self.credential, 'to_base_64') and callable(self.credential.to_base_64):
            headers['Authorization'] = f"Basic {self.credential.to_base_64()}"
        elif hasattr(self.credential, 'key'):
            headers['x-api-key'] = self.credential.key

        body = {
            "task_id": task_id,
            "source_type": source_type
        }
        if session_timeout_seconds is not None:
             body["session_timeout_seconds"] = session_timeout_seconds

        try:
            response = await self.client.post(
                session_url,
                json=body,
                headers=headers,
                timeout=10.0  # Session creation might take a bit longer
            )
            
            response.raise_for_status()
            
            # Parse the response to get the session ID
            response_data = response.json()
            if "session_id" in response_data:
                self.session_id = response_data["session_id"]
                logger.info(f"Successfully created session {self.session_id} for task: {task_id}")
                return response_data
            else:
                raise DriverError(f"Session creation response missing session_id: {response_data}")
                
        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to create session: {e.response.status_code}, Response: {e.response.text}")
            raise DriverError(f"HTTP Error {e.response.status_code} while creating session.")
        except httpx.RequestError as e:
            logger.error(f"Network error creating session: {e}")
            raise DriverError(f"Network error while creating session.")

    @retry(max_retries_attr='max_retries', delay_sec_attr='retry_delay_sec')
    async def heartbeat(self) -> Dict[str, Any]:
        """
        Sends a heartbeat to maintain session state with the sender endpoint.
        """
        session_id = self.session_id
        
        if not session_id:
            raise DriverError("Session ID is required for heartbeat, but session is not created.")
            
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
                logger.debug(f"Sender endpoint {self.endpoint} spec does not define 'x-fustor_agent-heartbeat-endpoint', using discovered path '{heartbeat_path}' from available endpoints.")
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

    
    @classmethod
    async def get_needed_fields(cls, **kwargs) -> Dict[str, Any]:
        """
        Implementation of the ABC method. Declares the data fields required by this sender.
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
    async def _get_spec(cls, client: httpx.AsyncClient, endpoint: str) -> Dict[str, Any]:
        """Fetch and cache the OpenAPI specification."""
        if endpoint in _spec_cache:
            return _spec_cache[endpoint]
            
        try:
            resp = await client.get(endpoint, timeout=10.0)
            resp.raise_for_status()
            spec = resp.json()
            _spec_cache[endpoint] = spec
            return spec
        except Exception as e:
            logger.error(f"Failed to fetch OpenAPI spec from {endpoint}: {e}")
            raise DriverError(f"Failed to fetch OpenAPI spec: {e}")

    @classmethod
    async def _get_all_post_endpoints_details(cls, client: httpx.AsyncClient, endpoint: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Get spec and filter for POST endpoints."""
        spec = await cls._get_spec(client, endpoint)
        post_endpoints = []
        
        paths = spec.get("paths", {})
        for path, details in paths.items():
            if "post" in details:
                post_op = details["post"]
                # Store minimal info needed
                post_endpoints.append({
                    "path": path,
                    "schema": post_op.get("requestBody", {}).get("content", {}).get("application/json", {}).get("schema", {})
                })
        return spec, post_endpoints
