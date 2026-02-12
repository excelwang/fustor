# tests/e2e/consistency/conftest.py
"""
Local conftest for the consistency E2E test suite.

Applies reset_fusion_state to all tests in this package.
The top-level conftest defines it as non-autouse so other
test packages (unit, contract) can opt out.
"""
import pytest


@pytest.fixture(autouse=True)
def _auto_reset(reset_fusion_state):
    """Automatically apply reset_fusion_state to every consistency test."""
    yield
