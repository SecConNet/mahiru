"""General support functions and fixtures for the tests.

This is a PyTest special file, see its documentation.
"""
import logging
from threading import Thread
from unittest.mock import patch
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer

from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend
import pytest

# TODO: the below hack will go when the complete API is implemented,
# then we'll have an object inside the fixture
from proof_of_concept.ddm_client import global_registry
from proof_of_concept.registry import Registry
from proof_of_concept.registry_api import RegistryApi

log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
logging.getLogger('filelock').setLevel(logging.WARNING)


@pytest.fixture
def clean_global_registry():
    """Create a fresh global registry for each test."""
    with patch('proof_of_concept.ddm_client.global_registry', Registry()):
        yield None


class ReusingWSGIServer(WSGIServer):
    """A simple WSGI server which allows reusing the port.

    There is of course a good reason for the existence of TIME_WAIT,
    but this is only used in testing, where we get in trouble when we
    try to create and destroy several registry servers in quick
    succession.
    """
    allow_reuse_address = True


@pytest.fixture
def registry_server():
    """Create a REST server instance for the global registry."""
    # Need to reimport, because we changed it in the fixture above
    # Will disappear, see above
    from proof_of_concept.ddm_client import global_registry
    api = RegistryApi(global_registry)
    server = ReusingWSGIServer(('0.0.0.0', 4413), WSGIRequestHandler)
    server.set_app(api.app)
    thread = Thread(
            target=server.serve_forever,
            name='RegistryServer')
    thread.start()

    yield

    server.shutdown()
    server.server_close()
    thread.join()


@pytest.fixture
def private_key():
    """Create a private RSA key."""
    return rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend())
