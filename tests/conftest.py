"""General support functions and fixtures for the tests.

This is a PyTest special file, see its documentation.
"""
from unittest.mock import patch

import pytest

from proof_of_concept.registry import Registry


@pytest.fixture
def clean_global_registry():
    """Create a fresh global registry for each test."""
    with patch('proof_of_concept.ddm_client.global_registry', Registry()):
        yield None
