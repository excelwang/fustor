"""
Logical Clock implementation for Fustor hybrid time synchronization.

This module provides a thread-safe logical clock that advances based on
observed file modification times (mtime). It's used to eliminate clock
drift issues between Agent, Fusion, and NFS servers.
"""
import threading
import time
from typing import Optional


class LogicalClock:
    """
    A thread-safe logical clock based on observed mtime values.
    
    The clock advances only when a newer mtime is observed. It provides
    a hybrid mode that returns max(logical_time, physical_time) to handle
    cold start scenarios.
    
    Usage:
        clock = LogicalClock()
        clock.update(file_mtime)  # Advances clock if mtime is newer
        current = clock.now()     # Get logical time
        hybrid = clock.hybrid_now()  # Get max(logical, physical)
    """
    
    def __init__(self, initial_time: float = 0.0):
        """
        Initialize the logical clock.
        
        Args:
            initial_time: Initial clock value (default 0.0)
        """
        self._value = initial_time
        self._lock = threading.Lock()
    
    def update(self, observed_mtime: float) -> float:
        """
        Update the logical clock with an observed mtime.
        
        The clock will only advance if the observed mtime is greater
        than the current clock value.
        
        Args:
            observed_mtime: The mtime value observed from a file
            
        Returns:
            The current clock value after the update
        """
        if observed_mtime is None:
            return self._value
            
        with self._lock:
            if observed_mtime > self._value:
                self._value = observed_mtime
            return self._value
    
    def now(self) -> float:
        """
        Get the current logical clock value.
        
        Returns:
            The current logical clock value (Unix timestamp)
        """
        with self._lock:
            return self._value
    
    def hybrid_now(self, fallback_to_physical: bool = True) -> float:
        """
        Get the hybrid clock value: max(logical, physical).
        
        This is useful for cold start scenarios where the logical clock
        may not have advanced yet but we still need a reasonable "now".
        
        Args:
            fallback_to_physical: If True, return max(logical, physical).
                                  If False, return logical clock only.
                                  
        Returns:
            The hybrid clock value
        """
        with self._lock:
            if fallback_to_physical:
                return max(self._value, time.time())
            return self._value
    
    def reset(self, value: float = 0.0) -> None:
        """
        Reset the logical clock to a specific value.
        
        Args:
            value: The value to reset to (default 0.0)
        """
        with self._lock:
            self._value = value
    
    def __repr__(self) -> str:
        return f"LogicalClock(value={self._value:.3f})"
