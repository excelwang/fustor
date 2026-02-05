"""
Pytest configuration for acceptance tests.

Provides fixtures to switch between Mock and Real system implementations.
"""
import pytest

from .protocol import UnifiedDataSystem
from .mock.fustor_mock import MockFustorSystem


def pytest_addoption(parser):
    """Add command-line option to select system implementation."""
    parser.addoption(
        "--system",
        action="store",
        default="mock",
        choices=["mock", "real"],
        help="Which system implementation to use: mock (fast) or real (integration)"
    )


@pytest.fixture
def unified_system(request) -> UnifiedDataSystem:
    """
    Fixture that provides the appropriate system implementation.
    
    Usage:
        pytest tests/acceptance/ --system=mock   # Use MockFustorSystem
        pytest tests/acceptance/ --system=real   # Use RealFustorSystem
    """
    system_type = request.config.getoption("--system")
    
    if system_type == "mock":
        system = MockFustorSystem()
    elif system_type == "real":
        # TODO: Implement RealFustorSystem adapter
        # from .adapters.real_system import RealFustorSystem
        # system = RealFustorSystem()
        pytest.skip("Real system adapter not yet implemented")
    else:
        raise ValueError(f"Unknown system type: {system_type}")
    
    yield system
    system.teardown()
