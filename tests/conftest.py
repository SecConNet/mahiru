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

from proof_of_concept.registry.registry import Registry
from proof_of_concept.rest.registry import RegistryRestApi

log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
logging.getLogger('filelock').setLevel(logging.WARNING)


class ReusingWSGIServer(WSGIServer):
    """A simple WSGI server which allows reusing the port.

    This disables the usual timeout the kernel imposes before you can
    reuse a server port (TIME_WAIT). We accept the reduced security
    here because this is only used in the test suite, and we don't want
    to wait several minutes in between tests for the port to free up.

    """
    allow_reuse_address = True


@pytest.fixture
def registry_server():
    """Create a REST server instance for the global registry."""
    registry = Registry()
    api = RegistryRestApi(registry)
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
