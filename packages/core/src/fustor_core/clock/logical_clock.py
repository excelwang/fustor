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
    
    It implements a Dual-Track Time System:
    - BaseLine: Driven by Fusion physical time minus Global Skew (Mode).
    - Trust Window: Allows fast-forwarding to mtime if within safe range (+1s).
    - Safeguard: Lower bounded by local physical time time.time() ONLY when uninitialized.
    """
    
    def __init__(self, initial_time: float = 0.0):
        """
        Initialize the logical clock.
        
        Args:
            initial_time: Initial clock value (default 0.0, will use time.time())
        """
        self._value = initial_time if initial_time > 0 else time.time()
        self._lock = threading.Lock()
        
        # --- Unified Global Clock State ---
        self._trust_window = 1.0  # seconds
        
        # Global Sample Buffer (Last 10,000 events)
        self._global_buffer: Deque[int] = deque(maxlen=10000)
        self._global_histogram: Counter = Counter()
        
        # Skew Calculation State
        self._cached_global_skew: Optional[int] = None
        self._dirty = False # If histogram changed, re-calc skew

    def update(self, observed_mtime: float, agent_time: Optional[float] = None, can_sample_skew: bool = True) -> float:
        """
        Update the logical clock.
        
        Args:
            observed_mtime: The mtime value observed from a file (NFS domain)
            agent_time: Optional. If provided, used for sampling. Defaults to Fusion Local Time.
            can_sample_skew: Whether this event is suitable for skew sampling (Realtime vs Audit)
            
        Returns:
            The current clock value after the update
        """
        # Unified physical reference: Use Fusion local time if no agent_time or if prioritizing stability
        # Note: Using Fusion time here makes the system immune to Agent local clock errors.
        reference_time = time.time() if agent_time is None else agent_time
        
        with self._lock:
            # --- Special Case: Deletion/Metadata event (observed_mtime is None) ---
            if observed_mtime is None:
                effective_skew = self._get_global_skew_locked()
                
                if effective_skew is not None:
                    # Advance clock to BaseLine to reflect physical progress in Fusion Domain
                    baseline = reference_time - effective_skew
                    if baseline > self._value:
                        self._value = baseline
                return self._value

            # --- Robust Sampling Mode ---
            try:
                # 1. Update Global Statistics
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
                
                # 2. Get Global Skew (Stable Mode)
                effective_skew = self._get_global_skew_locked()
                
                # 3. Calculate Watermark Candidates
                if effective_skew is None:
                    # Fallback: No statistics yet
                    if observed_mtime > self._value:
                        self._value = observed_mtime
                else:
                    # BaseLine = FusionPhysicalTime - Skew
                    baseline = reference_time - effective_skew
                    
                    # Trust Window Logic
                    upper_bound = baseline + self._trust_window
                    
                    target_value = self._value # Start with current
                    
                    # Logic Table from Design Doc
                    if observed_mtime <= self._value:
                        # Past data, ignore for clock movement
                        pass
                    elif baseline < observed_mtime <= upper_bound:
                        # [Fast Path] mtime is within [BaseLine, BaseLine + 1.0s]
                        # Trust the mtime directly for maximum real-time precision
                        target_value = observed_mtime
                    elif observed_mtime > upper_bound:
                        # [Future/Anomaly] Advance to BaseLine only
                        # Protects against 'touch' future files or massive clock jumps
                        if baseline > self._value:
                            target_value = baseline
                    else:
                        # observed_mtime <= baseline (but > current value)
                        target_value = observed_mtime

                    # Monotonicity check
                    if target_value > self._value:
                        self._value = target_value
                    
                    # ENFORCE BASELINE: Even if mtime is old (past data), the clock must flow with physical time.
                    # This fixes the "Stagnation" issue (Spec Section 4.1) where lack of new writes
                    # caused the watermark to freeze, making old files look "fresh" (0 age).
                    if baseline > self._value:
                        self._value = baseline
            except Exception as e:
                # Silent fail to proceed with event processing
                pass

            return self._value

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
        Get the current logical clock value (Observation Watermark).
        Fallback: Returns time.time() ONLY if clock is completely uninitialized (0.0).
        """
        with self._lock:
            if self._value == 0.0:
                return time.time()
            return self._value
            
    def get_watermark(self) -> float:
        return self.now()
    
    def reset(self, value: float = 0.0) -> None:
        with self._lock:
            self._value = value if value > 0 else time.time()
            self._global_buffer.clear()
            self._global_histogram.clear()
            self._cached_global_skew = None
            self._dirty = False
    
    def __repr__(self) -> str:
        skew = self._cached_global_skew if self._cached_global_skew is not None else "N/A"
        return f"LogicalClock(val={self._value:.3f}, skew={skew}, samples={len(self._global_buffer)})"
