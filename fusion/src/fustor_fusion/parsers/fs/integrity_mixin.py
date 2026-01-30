import time
import heapq
import os
from typing import Dict
from .base import ParserBase

class IntegrityMixin(ParserBase):
    async def cleanup_expired_suspects(self):
        """Public method called by background task to trigger periodic cleanup."""
        async with self._global_semaphore:
            self._cleanup_expired_suspects_unlocked()

    def _cleanup_expired_suspects_unlocked(self):
        """Clean up expired suspects and their flags. Must be called with lock held."""
        if not self._suspect_list:
            return 0
            
        now_monotonic = time.monotonic()
        
        if now_monotonic - self._last_suspect_cleanup_time < self._suspect_cleanup_interval:
            return 0
            
        self._last_suspect_cleanup_time = now_monotonic
        processed_count = 0
        
        while self._suspect_heap and self._suspect_heap[0][0] <= now_monotonic:
            expires_at, path = heapq.heappop(self._suspect_heap)
            processed_count += 1
            
            if path not in self._suspect_list:
                continue
            
            current_expiry, recorded_mtime = self._suspect_list[path]
            if abs(current_expiry - expires_at) > 1e-6:
                continue
            
            node = self._get_node(path)
            if not node:
                del self._suspect_list[path]
                continue
            
            current_mtime = node.modified_time
            if abs(current_mtime - recorded_mtime) > 1e-6:
                new_expiry = now_monotonic + self.hot_file_threshold
                self._suspect_list[path] = (new_expiry, current_mtime)
                heapq.heappush(self._suspect_heap, (new_expiry, path))
                self.logger.info(f"Suspect RENEWED (Active): {path} (new mtime: {current_mtime})")
            else:
                self.logger.info(f"Suspect EXPIRED (Stable): {path}")
                del self._suspect_list[path]
                node.integrity_suspect = False
        
        return processed_count

    async def update_suspect(self, path: str, new_mtime: float):
        """Update a suspect file's mtime from Sentinel Sweep."""
        path = os.path.normpath(path).rstrip('/') if path != '/' else '/'
        async with self._global_semaphore:
            async with self._get_segment_lock(path):
                self._logical_clock.update(new_mtime)
                
                node = self._get_node(path)
                if not node:
                    self.logger.warning(f"Suspect update ignored: {path} not found in tree")
                    return

                old_mtime = node.modified_time
                node.modified_time = new_mtime
                watermark = self._logical_clock.get_watermark()
                
                if old_mtime is None or abs(new_mtime - old_mtime) > 1e-6:
                    if (watermark - new_mtime) < self.hot_file_threshold:
                        if path not in self._suspect_list:
                            expiry = time.monotonic() + self.hot_file_threshold
                            self._suspect_list[path] = (expiry, new_mtime)
                            heapq.heappush(self._suspect_heap, (expiry, path))
                        node.integrity_suspect = True
                    else:
                        node.integrity_suspect = False
                        self._suspect_list.pop(path, None)
                else:
                    # mtime same (Confirmed stable)
                    if (watermark - new_mtime) >= self.hot_file_threshold:
                        node.integrity_suspect = False
                        self._suspect_list.pop(path, None)
