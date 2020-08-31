"""General support functions and fixtures for the tests.

This is a PyTest special file, see its documentation.
"""
import logging
from unittest.mock import patch

from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend
import pytest

from proof_of_concept.registry import Registry

log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)


@pytest.fixture
def clean_global_registry():
    """Create a fresh global registry for each test."""
    with patch('proof_of_concept.ddm_client.global_registry', Registry()):
        yield None


@pytest.fixture
def private_key():
    """Create a private RSA key."""
    return rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend())
