"""
Observability and Metrics abstraction.

This module provides a decoupled way to record metrics throughout the Fustor system.
"""
from abc import ABC, abstractmethod
from typing import Dict, Optional, Any
import logging

logger = logging.getLogger(__name__)

class Metrics(ABC):
    """Abstract base class for metrics recording."""
    
    @abstractmethod
    def counter(self, name: str, value: float = 1.0, tags: Optional[Dict[str, str]] = None) -> None:
        """Increment a counter."""
        pass

    @abstractmethod
    def gauge(self, name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        """Set a gauge value."""
        pass

    @abstractmethod
    def histogram(self, name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        """Record a histogram value."""
        pass

class NoOpMetrics(Metrics):
    """Default implementation that does nothing (or logs debug)."""
    
    def counter(self, name: str, value: float = 1.0, tags: Optional[Dict[str, str]] = None) -> None:
        pass

    def gauge(self, name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        pass

    def histogram(self, name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        pass

class LoggingMetrics(Metrics):
    """Metrics implementation that logs to standard python logger (useful for debugging)."""
    
    def __init__(self, logger_name: str = "fustor.metrics"):
        self.logger = logging.getLogger(logger_name)

    def _format_tags(self, tags: Optional[Dict[str, str]]) -> str:
        if not tags:
            return ""
        return " ".join([f"{k}={v}" for k, v in tags.items()])

    def counter(self, name: str, value: float = 1.0, tags: Optional[Dict[str, str]] = None) -> None:
        tag_str = self._format_tags(tags)
        self.logger.debug(f"METRIC counter {name} +{value} {tag_str}")

    def gauge(self, name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        tag_str = self._format_tags(tags)
        self.logger.debug(f"METRIC gauge {name} ={value} {tag_str}")

    def histogram(self, name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        tag_str = self._format_tags(tags)
        self.logger.debug(f"METRIC histogram {name} : {value} {tag_str}")

_GLOBAL_METRICS: Metrics = NoOpMetrics()

def get_metrics() -> Metrics:
    """Get the global metrics instance."""
    return _GLOBAL_METRICS

def set_global_metrics(metrics: Metrics) -> None:
    """Set the global metrics instance."""
    global _GLOBAL_METRICS
    _GLOBAL_METRICS = metrics
