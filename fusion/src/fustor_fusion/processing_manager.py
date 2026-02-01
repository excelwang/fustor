import asyncio
import logging
from typing import Dict, List, Optional, Any
from collections import defaultdict
from .queue_integration import get_events_from_queue
from .view_manager.manager import process_event as process_single_event
from .runtime import datastore_event_manager
from .in_memory_queue import memory_event_queue

logger = logging.getLogger(__name__)

class ProcessingManager:
    def __init__(self):
        self._tasks: Dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()
        # 追踪每个数据存储正在处理中（已出队但未完成解析）的事件数量
        self._inflight_counts: Dict[str, int] = defaultdict(int)

    def get_inflight_count(self, datastore_id: str) -> int:
        """获取当前正在内存解析中的事件数"""
        return self._inflight_counts.get(str(datastore_id), 0)

    async def ensure_processor(self, datastore_id: str):
        """确保指定数据存储的处理循环正在运行"""
        ds_id_str = str(datastore_id)
        async with self._lock:
            if ds_id_str not in self._tasks or self._tasks[ds_id_str].done():
                task = asyncio.create_task(self._per_datastore_processing_loop(ds_id_str))
                self._tasks[ds_id_str] = task
                logger.info(f"Started dynamic background processing task for datastore {ds_id_str}.")

    async def sync_tasks(self, datastore_configs: Dict[str, Any]):
        """根据配置同步处理任务 (由 reload_configs_job 调用)"""
        for ds_id, config in datastore_configs.items():
            await self.ensure_processor(ds_id)

    async def _per_datastore_processing_loop(self, datastore_id: str):
        """优化的后台处理循环：事件驱动 + 状态追踪"""
        ds_id_str = str(datastore_id)
        log = logging.getLogger(f"fustor.processor.{ds_id_str}")
        log.info(f"Processing loop started for datastore {ds_id_str}")
        
        while True:
            try:
                # 1. 尝试获取一批事件
                events = await get_events_from_queue(datastore_id)
                
                if not events:
                    # 2. 如果队列为空，挂起等待唤醒
                    await datastore_event_manager.wait_for_event(datastore_id)
                    continue

                # --- 关键：增加处理中计数 ---
                count = len(events)
                self._inflight_counts[ds_id_str] += count
                
                try:
                    processed_count = 0
                    for event_obj in events:
                        try:
                            await process_single_event(event_obj, ds_id_str)
                            processed_count += 1
                        except Exception as e:
                            log.error(f"Error processing event: {e}", exc_info=True)
                    
                    if processed_count > 0:
                        log.info(f"Processed {processed_count} events. Queue: {memory_event_queue.get_queue_size(ds_id_str)}")
                finally:
                    # --- 关键：无论成功失败，处理完后减去计数 ---
                    self._inflight_counts[ds_id_str] -= count
                
                # 给予其他协程运行机会
                await asyncio.sleep(0)

            except asyncio.CancelledError:
                log.info(f"Processing loop for datastore {datastore_id} stopped.")
                break
            except Exception as e:
                log.error(f"Critical error in processing loop: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def stop_all(self):
        async with self._lock:
            for ds_id, task in self._tasks.items():
                task.cancel()
            if self._tasks:
                await asyncio.gather(*self._tasks.values(), return_exceptions=True)
            self._tasks.clear()
            self._inflight_counts.clear()

# 全局单例
processing_manager = ProcessingManager()