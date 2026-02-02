import asyncio
import collections
import logging
from typing import Dict, Any, List, Optional, TYPE_CHECKING, Tuple
from uuid import uuid4
import threading
from typing import Set

class RequiredFieldsTracker:
    def __init__(self):
        self._required_fields: Set[str] = set()
        self._lock = threading.Lock()
        self._event = threading.Event()

    def get_fields(self) -> Set[str]:
        with self._lock:
            return self._required_fields.copy()

    def update_fields(self, new_fields: Set[str]):
        with self._lock:
            if self._required_fields != new_fields:
                self._required_fields = new_fields
                self._event.set()

    def clear_event(self):
        self._event.clear()

    def wait_for_change(self, timeout: Optional[float] = None) -> bool:
        return self._event.wait(timeout)

from .base import BaseInstanceService
from fustor_core.models.states import EventBusInstance, EventBusState
from fustor_core.models.config import SourceConfig, FieldMapping
from fustor_agent.runtime.bus import MemoryEventBus
from fustor_core.exceptions import ConfigError, NotFoundError, DriverError, TransientSourceBufferFullError

if TYPE_CHECKING:
    from fustor_agent.services.instances.pipeline import PipelineInstanceService
    from fustor_agent.services.drivers.source_driver import SourceDriverService

logger = logging.getLogger("fustor_agent")
from fustor_agent_sdk.interfaces import EventBusServiceInterface # Import the interface

class EventBusInstanceRuntime:
    def __init__(self, source_id, source_config: SourceConfig, source_signature: Any, source_driver_service: "SourceDriverService", initial_start_position: int = 0, loop: Optional[asyncio.AbstractEventLoop] = None):
        self.id: str = f"bus-{uuid4()}"
        self.source_id: str = source_id
        self.config: SourceConfig = source_config
        self.source_signature = source_signature
        
        source_driver_class = source_driver_service._get_driver_by_type(self.config.driver)
        self.source_driver_instance = source_driver_class(source_id, self.config)

        self.internal_bus: MemoryEventBus = MemoryEventBus(
            bus_id=self.id, 
            capacity=source_config.max_queue_size, 
            start_position=initial_start_position
        )
        
        self.state: EventBusState = EventBusState.IDLE
        self.info: str = "总线已创建，等待订阅者以启动数据生产。"
        self.statistics: Dict[str, Any] = {"events_produced": 0, "subscriber_count": 0}
        self.metrics: Dict[str, Any] = {"events_produced_total": 0, "bus_queue_size": 0}
        self.producer_task: Optional[asyncio.Task] = None
        self._producer_stop_event = threading.Event()
        self.required_fields_tracker = RequiredFieldsTracker()
        self.needed_position_lost: bool = False # Add new flag
        self._loop = loop or asyncio.get_running_loop()
        self.ready_event = asyncio.Event()

    def update_driver_required_fields(self, new_fields: Set[str]):
        self.required_fields_tracker.update_fields(new_fields)

    async def start_producer(self):
        if self.producer_task and not self.producer_task.done():
            logger.warning(f"总线 '{self.id}' 的生产者任务已在运行。")
            return

        self.state = EventBusState.PRODUCING
        self.info = "生产者任务已启动，正在监听数据源。"
        self._producer_stop_event.clear()
        self.producer_task = asyncio.create_task(self._produce_loop())
        logger.info(self.info)

    async def stop_producer(self):
        if self.producer_task and not self.producer_task.done():
            self._producer_stop_event.set()
            self.producer_task.cancel()
            try:
                await self.producer_task
            except asyncio.CancelledError:
                pass
        self.state = EventBusState.IDLE
        self.info = "因没有订阅者，生产者任务已暂停。"
        self.producer_task = None
        logger.info(self.info)

    async def _produce_loop(self):
        logger.info(f"总线 '{self.id}' 的生产循环已启动。")
        import queue
        event_queue = queue.Queue(maxsize=100)

        def _threaded_producer():
            logger.info(f"总线 '{self.id}' 的后台生产者线程已启动。")
            try:
                # Check if the requested position is available before getting the iterator
                start_position = self.internal_bus.buffer_start_position
                self.needed_position_lost = not self.source_driver_instance.is_position_available(start_position)
                
                # Get the iterator with the appropriate start position
                iterator = self.source_driver_instance.get_message_iterator(
                    start_position=start_position if not self.needed_position_lost else -1,
                    stop_event=self._producer_stop_event,
                    required_fields_tracker=self.required_fields_tracker
                )
                
                # Signal that the producer is ready (e.g., pre-scan is done and iterator is obtained)
                self._loop.call_soon_threadsafe(self.ready_event.set)
                logger.info(f"总线 '{self.id}' 的后台生产者线程已就绪，进入事件循环。")

                for event in iterator:
                    if self._producer_stop_event.is_set() or self.source_driver_instance._stop_driver_event.is_set(): # Check new stop event
                        break
                    event_queue.put(event)
            except DriverError as e: # Catch the specific DriverError
                logger.error(f"总线 '{self.id}' 的后台生产者因驱动错误而停止: {e}", exc_info=True)
                event_queue.put(e) # Propagate the error
            except Exception as e:
                logger.error(f"总线 '{self.id}' 的后台生产者出现异常: {e}", exc_info=True)
                event_queue.put(e)
            finally:
                # Always put None to unblock the consumer, regardless of how the loop exited.
                if not self.ready_event.is_set():
                    self._loop.call_soon_threadsafe(self.ready_event.set)
                event_queue.put(None)
                logger.info(f"总线 '{self.id}' 的后台生产者线程已结束。")

        producer_thread = threading.Thread(target=_threaded_producer, daemon=True)
        producer_thread.start()

        try:
            while True:
                event = await asyncio.to_thread(event_queue.get)

                if event is None:
                    logger.info(f"总线 '{self.id}' 的生产者已发出完成信号。")
                    break
                
                if isinstance(event, Exception):
                    if isinstance(event, DriverError):
                        error_msg = f"总线 '{self.id}' 的生产循环因驱动错误而停止: {event}"
                        self.state = EventBusState.ERROR
                        self.info = error_msg
                        logger.error(error_msg, exc_info=True)
                        self.internal_bus.mark_as_failed(str(event))
                        break # Stop the loop on DriverError
                    else:
                        raise event

                await self.internal_bus.put(event, is_transient=self.source_driver_instance.is_transient)
                if hasattr(event, 'rows'):
                    self.statistics["events_produced"] += len(event.rows)
                    self.metrics["events_produced_total"] += len(event.rows)
                self.metrics["bus_queue_size"] = len(self.internal_bus.buffer)

        except asyncio.CancelledError:
            logger.info(f"总线 '{self.id}' 的生产循环被取消。")
        except TransientSourceBufferFullError as e:
            error_msg = str(e)
            self.state = EventBusState.ERROR
            self.info = error_msg
            logger.error(f"总线 '{self.id}' 因缓冲区满而停止: {error_msg}")
            self.internal_bus.mark_as_failed(error_msg)
        except Exception as e:
            error_msg = f"总线 '{self.id}' 的生产循环崩溃: {e}"
            self.state = EventBusState.ERROR
            self.info = error_msg
            logger.error(error_msg, exc_info=True)
            self.internal_bus.mark_as_failed(str(e))
        finally:
            self._producer_stop_event.set()
            if producer_thread.is_alive():
                await asyncio.to_thread(producer_thread.join, timeout=5.0)
            logger.info(f"总线 '{self.id}' 的生产循环已结束。")

    def get_dto(self) -> EventBusInstance:
        self.statistics['buffer_size'] = len(self.internal_bus.buffer)
        self.statistics['subscriber_count'] = self.internal_bus.get_subscriber_count()
        self.statistics.update(self.metrics)
        return EventBusInstance(
            id=self.id,
            source_name=self.source_id,
            state=self.state,
            info=self.info,
            statistics=self.statistics
        )

class EventBusService(BaseInstanceService, EventBusServiceInterface): # Inherit from the interface
    def __init__(self, source_configs: Dict[str, SourceConfig], source_driver_service: "SourceDriverService"):
        super().__init__()
        self.source_configs = source_configs
        self.source_driver_service = source_driver_service
        self.bus_by_signature: Dict[Any, EventBusInstanceRuntime] = {}
        self._bus_creation_locks: Dict[str, asyncio.Lock] = collections.defaultdict(asyncio.Lock)
        self.pipeline_instance_service: Optional["PipelineInstanceService"] = None

    def set_dependencies(self, pipeline_instance_service: "PipelineInstanceService"):
        self.pipeline_instance_service = pipeline_instance_service

    def _generate_source_signature(self, source_config: SourceConfig) -> Any:
        return (source_config.driver, source_config.uri, source_config.credential)

    async def get_or_create_bus_for_subscriber(
        self, 
        source_id: str,
        source_config: SourceConfig, 
        pipeline_id: str,
        required_position: int,
        fields_mapping: List[FieldMapping],
        force_new: bool = False
    ) -> Tuple[EventBusInstanceRuntime, bool]: # Updated return type
        source_signature = self._generate_source_signature(source_config)
        
        async with self._bus_creation_locks[str(source_signature)]:
            bus_runtime = self.bus_by_signature.get(source_signature)

            if force_new:
                logger.info(f"Forcing creation of a new bus for signature {source_signature} due to split.")
                bus_runtime = None
            elif bus_runtime and bus_runtime.state == EventBusState.ERROR:
                logger.warning(f"Removed failed EventBus '{bus_runtime.id}' for source '{source_id}'. Creating a new one.")
                self.pool.pop(bus_runtime.id, None)
                bus_runtime = None

            if bus_runtime and not bus_runtime.internal_bus.can_subscribe(required_position):
                logger.info(f"Existing bus '{bus_runtime.id}' is unsuitable for pipeline '{pipeline_id}' (requires position {required_position}, bus starts at {bus_runtime.internal_bus.buffer_start_position}). Creating a new bus.")
                bus_runtime = None

            if not bus_runtime:
                bus_runtime = EventBusInstanceRuntime(
                    source_id=source_id,
                    source_config=source_config, 
                    source_signature=source_signature, 
                    source_driver_service=self.source_driver_service,
                    initial_start_position=required_position,
                    loop=asyncio.get_running_loop()
                )
                self.pool[bus_runtime.id] = bus_runtime
                self.bus_by_signature[source_signature] = bus_runtime

        # Ensure the producer is running for the bus we are about to use.
        # This is crucial for recovered buses that don't have an active producer task.
        if not bus_runtime.producer_task or bus_runtime.producer_task.done():
            await bus_runtime.start_producer()

        # All callers must wait for the producer to be ready before proceeding
        await bus_runtime.ready_event.wait()

        await bus_runtime.internal_bus.subscribe(pipeline_id, required_position, fields_mapping)
        bus_runtime.update_driver_required_fields(bus_runtime.internal_bus.required_fields)
        
        # Return the tuple with the signal
        return bus_runtime, bus_runtime.needed_position_lost

    async def release_subscriber(self, bus_id: str, pipeline_id: str):
        bus_runtime = self.get_instance(bus_id)
        if not bus_runtime:
            logger.warning(f"Attempted to release subscriber from non-existent bus '{bus_id}'.")
            return
        
        await bus_runtime.internal_bus.unsubscribe(pipeline_id)
        
        if bus_runtime.internal_bus.get_subscriber_count() == 0:
            logger.info(f"Bus '{bus_id}' has no more subscribers, stopping its producer.")
            await bus_runtime.stop_producer()
            self.pool.pop(bus_id, None)
            if bus_runtime.source_signature in self.bus_by_signature:
                del self.bus_by_signature[bus_runtime.source_signature]

    async def release_all_unused_buses(self):
        logger.info("Releasing all unused event buses...")
        for bus_id in list(self.pool.keys()):
            bus = self.pool.get(bus_id)
            if bus and bus.internal_bus.get_subscriber_count() == 0:
                await bus.stop_producer()
                self.pool.pop(bus_id, None)
                if bus.source_signature in self.bus_by_signature:
                    del self.bus_by_signature[bus.source_signature]

    async def commit_and_handle_split(
        self, 
        bus_id: str, 
        pipeline_id: str, 
        num_events: int, 
        last_consumed_position: int,
        fields_mapping: List[FieldMapping]
    ):
        bus_runtime = self.get_instance(bus_id)
        if not bus_runtime:
            return

        task_to_split_id = await bus_runtime.internal_bus.commit(pipeline_id, num_events, last_consumed_position)

        if task_to_split_id:
            logger.warning(f"Bus split condition met on '{bus_id}'. Task '{task_to_split_id}' will be migrated.")
            
            source_config = self.source_configs.get(bus_runtime.source_id)
            if not source_config:
                logger.error(f"Cannot split task: Source config '{bus_runtime.source_id}' not found.")
                return

            # When splitting, the new bus must also return the signal
            new_bus, needed_position_lost = await self.get_or_create_bus_for_subscriber(
                source_id=bus_runtime.source_id,
                source_config=source_config,
                pipeline_id=task_to_split_id,
                required_position=last_consumed_position,
                fields_mapping=fields_mapping,
                force_new=True
            )
            
            # The remapping pipeline instance needs to know about the signal too
            await self.pipeline_instance_service.remap_pipeline_to_new_bus(task_to_split_id, new_bus, needed_position_lost)