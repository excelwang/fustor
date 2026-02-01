"""
Logical Clock implementation for Fustor hybrid time synchronization.

This module provides a thread-safe logical clock that advances based on
observed file modification times (mtime). It's used to eliminate clock
drift issues between Agent, Fusion, and NFS servers.
"""
import threading
import time
import statistics
from typing import Optional, Dict, Deque
from collections import deque, Counter, defaultdict

class LogicalClock:
    """
    A robust logical clock that advances based on statistical analysis of 
    Agent Time vs Observed Mtime skew.
    
    It implements a Dual-Track Time System:
    - BaseLine: Driven by Agent physical time minus Global Skew (Mode).
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
        
        # --- Robust Clock State ---
        self._trust_window = 1.0  # seconds
        
        # Session Skew Tracking
        # Key: session_id, Value: Deque of last 1000 diffs (agent_time - mtime)
        self._session_buffers: Dict[str, Deque[int]] = {}
        self._MAX_SAMPLES = 1000
        
        # Global Histogram for O(1) Mode calculation
        # Key: Skew (int), Value: Count
        self._global_histogram: Counter = Counter()
        
        # Caches
        self._cached_global_skew: Optional[int] = None
        self._dirty = False # If histogram changed, re-calc skew

    def update(self, observed_mtime: float, agent_time: Optional[float] = None, session_id: Optional[str] = None, can_sample_skew: bool = True) -> float:
        """
        Update the logical clock.
        
        Supports two modes:
        1. Legacy/Simple: update(mtime) -> uses mtime blindly (Max).
           Used for Snapshot/Audit or when agent_time is unavailable.
        2. Robust: update(mtime, agent_time, session_id)
           Uses the statistical algorithm.
        
        Args:
            observed_mtime: The mtime value observed from a file
            agent_time: The physical timestamp when the event was generated (event.index)
            session_id: The session ID of the source agent
            can_sample_skew: Whether this event is suitable for skew sampling (Realtime vs Audit)
            
        Returns:
            The current clock value after the update
        """
        
        with self._lock:
            # --- Legacy Mode (Fallback if NO agent_time provided) ---
            if agent_time is None:
                # If we have NO history (clock is 0), we must initialize using mtime
                if self._value == 0.0:
                     self._value = observed_mtime if observed_mtime is not None else 0.0
                elif observed_mtime is not None and observed_mtime > self._value:
                     # Legacy behavior: blindly advance to newer mtime
                     self._value = observed_mtime
                
                return self._value

            # Use a default session_id if none provided
            if session_id is None:
                session_id = "_default_local_session"
            
            # --- Special Case: Deletion/Metadata event (observed_mtime is None) ---
            if observed_mtime is None:
                session_skew = self._get_session_skew_locked(session_id)
                g_skew = self._get_global_skew_locked()
                effective_skew = session_skew if session_skew is not None else g_skew
                
                if effective_skew is not None:
                    # Advance clock to BaseLine to reflect physical progress
                    baseline = agent_time - effective_skew
                    if baseline > self._value:
                        self._value = baseline
                return self._value

            # --- Robust Mode ---
            try:
                # 1. Update Statistics (ONLY if it's a realtime/fresh observation)
                if can_sample_skew and agent_time is not None and observed_mtime is not None:
                    diff = int(agent_time - observed_mtime)
                
                    if session_id not in self._session_buffers:
                        self._session_buffers[session_id] = deque(maxlen=self._MAX_SAMPLES)
                    
                    buf = self._session_buffers[session_id]
                    
                    # If buffer full, remove old sample from histogram
                    if len(buf) == self._MAX_SAMPLES:
                        old_val = buf[0]
                        buf.popleft() 
                        
                        if old_val in self._global_histogram:
                            self._global_histogram[old_val] -= 1
                            if self._global_histogram[old_val] <= 0:
                                del self._global_histogram[old_val]
                    
                    buf.append(diff)
                    self._global_histogram[diff] += 1
                    self._dirty = True
                
                # 2. Get Appropriate Skew (Prefer current session's mode for physical anchoring)
                session_skew = self._get_session_skew_locked(session_id)
                g_skew = self._get_global_skew_locked()
                
                # Use session skew if available, otherwise global fallback
                effective_skew = session_skew if session_skew is not None else g_skew
                
                # 3. Calculate Watermark Candidates
                if effective_skew is None:
                    # Fallback: No statistics yet
                    if observed_mtime is not None and observed_mtime > self._value:
                        self._value = observed_mtime
                else:
                    # BaseLine = AgentTime - Skew
                    baseline = agent_time - effective_skew
                    
                    # Trust Window Logic
                    upper_bound = baseline + self._trust_window
                    
                    target_value = self._value # Start with current
                    
                    # Logic Table from Design Doc
                    if observed_mtime <= self._value:
                        # Past data, ignore for clock
                        pass
                    elif baseline < observed_mtime <= upper_bound:
                        # [Fast Path] mtime is within [BaseLine, BaseLine + 1.0s]
                        target_value = observed_mtime
                    elif observed_mtime > upper_bound:
                         # [Future/Anomaly] Advance to BaseLine only
                         if baseline > self._value:
                             target_value = baseline
                    else:
                        # observed_mtime <= baseline (but > current)
                        target_value = observed_mtime

                    # Monotonicity check
                    if target_value > self._value:
                        self._value = target_value
            except Exception as e:
                # Log error but return current value to proceed with event processing
                print(f"ERROR: LogicalClock update failed: {e}")
                pass

            return self._value

    def _get_session_skew_locked(self, session_id: Optional[str]) -> Optional[int]:
        """Calculates skew for a specific session."""
        if not session_id or session_id not in self._session_buffers:
            return None
        buf = self._session_buffers[session_id]
        if not buf:
            return None
        return statistics.mode(buf)

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
        
        # Tie-breaker logic
        max_freq = most_common[0][1]
        candidates = [k for k, count in most_common if count == max_freq]
        
        # Tie-breaker: smallest skew (conservative latency)
        best_skew = min(candidates)
        
        self._cached_global_skew = best_skew
        self._dirty = False
        return best_skew

    def remove_session(self, session_id: str):
        """Clean up statistics for a closed session."""
        with self._lock:
            if session_id in self._session_buffers:
                buf = self._session_buffers[session_id]
                for sample in buf:
                    if sample in self._global_histogram:
                        self._global_histogram[sample] -= 1
                        if self._global_histogram[sample] <= 0:
                            del self._global_histogram[sample]
                del self._session_buffers[session_id]
                self._dirty = True

    def now(self) -> float:
        """
        Get the current logical clock value (Observation Watermark).
        
        Returns:
            The current calculated watermark.
            Fallback: Returns time.time() ONLY if clock is uninitialized (0.0).
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
            self._session_buffers.clear()
            self._global_histogram.clear()
            self._cached_global_skew = None
    
    def __repr__(self) -> str:
        skew = self._cached_global_skew if self._cached_global_skew is not None else "N/A"
        return f"LogicalClock(val={self._value:.3f}, skew={skew}, samples={sum(self._global_histogram.values())})"
