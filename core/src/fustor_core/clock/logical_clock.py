"""
Logical Clock implementation for Fustor hybrid time synchronization.

This module provides a thread-safe logical clock that advances based on
observed file modification times (mtime). It uses Fusion Local Time as the 
authority to eliminate clock drift issues across distributed Agents.
"""
import threading
import time
from typing import Optional, Dict, Deque
from collections import deque, Counter

class LogicalClock:
    """
    A robust logical clock that advances based on a UNIFIED statistical analysis 
    of Fusion Local Time vs Observed Mtime skew.
    
    It implements a simplified time system:
    - Watermark = Fusion_Physical_Time - Mode_Skew
    - Skew is calculated as the mode of (reference_time - mtime) samples
    - Completely immune to mtime manipulation (touch -d future)
    """
    
    def __init__(self, initial_time: float = 0.0):
        """
        Initialize the logical clock.
        
        Args:
            initial_time: Unused, kept for backward compatibility.
        """
        # Note: threading.RLock is used because this class serves both
        # Agent-side (multi-threaded scanning) and Fusion-side (asyncio) contexts.
        self._lock = threading.RLock()

        
        # --- Unified Global Clock State ---
        
        # Global Sample Buffer (Last 10,000 events)
        self._global_buffer: Deque[int] = deque(maxlen=10000)
        self._global_histogram: Counter = Counter()
        
        # Skew Calculation State
        self._cached_global_skew: Optional[int] = None
        self._dirty = False # If histogram changed, re-calc skew

    def update(self, observed_mtime: float, can_sample_skew: bool = True) -> float:
        """
        Update the logical clock by sampling skew from observed mtime.
        
        Args:
            observed_mtime: The mtime value observed from a file (NFS domain)
            can_sample_skew: Whether this event is suitable for skew sampling (Realtime vs Audit)
            
        Returns:
            The current watermark value after the update
        
        Note:
            Watermark is now purely `Fusion_Physical_Time - Mode_Skew`.
            The old Trust Window / Fast Path logic has been removed for simplicity and immunity.
        """
        # Unified physical reference: Always use Fusion Local Time (Spec §4.1.A)
        # This makes the system immune to Agent local clock errors (Faketime/NTP drift).
        reference_time = time.time()
        
        with self._lock:
            # --- Special Case: Deletion/Metadata event (observed_mtime is None) ---
            if observed_mtime is None:
                return self.get_watermark()

            # --- Skew Sampling Only ---
            try:
                if can_sample_skew:
                    diff = int(reference_time - observed_mtime)
                    
                    # If buffer full, remove old sample from histogram
                    if len(self._global_buffer) == self._global_buffer.maxlen:
                        old_val = self._global_buffer[0]
                        self._global_histogram[old_val] -= 1
                        if self._global_histogram[old_val] <= 0:
                            del self._global_histogram[old_val]
                    
                    self._global_buffer.append(diff)
                    self._global_histogram[diff] += 1
                    self._dirty = True
            except Exception:
                # Silent fail to proceed with event processing
                pass

            return self.get_watermark()


    def _get_global_skew_locked(self) -> Optional[int]:
        """Recalculate or return cached global skew (Mode)."""
        if not self._dirty and self._cached_global_skew is not None:
             return self._cached_global_skew
        
        if not self._global_histogram:
            return None
            
        # Find Mode (Most Common Skew)
        most_common = self._global_histogram.most_common()
        if not most_common:
             return None
        
        # Tie-breaker logic: If frequencies are equal, take the smallest skew (lower latency)
        max_freq = most_common[0][1]
        candidates = [k for k, count in most_common if count == max_freq]
        best_skew = min(candidates)
        
        self._cached_global_skew = best_skew
        self._dirty = False
        return best_skew

    def now(self) -> float:
        """
        Get the current watermark value.
        
        Formula: Fusion_Physical_Time - Mode_Skew
        
        When skew is not yet calibrated (cold start), falls back to physical time.
        """
        with self._lock:
            effective_skew = self._get_global_skew_locked()
            if effective_skew is not None:
                return time.time() - effective_skew
            # Cold start: no skew samples yet, use physical time
            return time.time()
            
    def get_watermark(self) -> float:
        """Get the current watermark (alias for now())."""
        return self.now()


    def get_skew(self) -> float:
        """
        Return the current estimated Global Skew (Mode).
        Returns 0.0 if not yet calibrated.
        """
        with self._lock:
             return float(self._get_global_skew_locked() or 0.0)
    
    def reset(self, value: float = 0.0) -> None:
        with self._lock:
            self._global_buffer.clear()
            self._global_histogram.clear()
            self._cached_global_skew = None
            self._dirty = False
    
    def __repr__(self) -> str:
        skew = self._cached_global_skew if self._cached_global_skew is not None else "N/A"
        watermark = self.get_watermark()
        return f"LogicalClock(watermark={watermark:.3f}, skew={skew}, samples={len(self._global_buffer)})"
