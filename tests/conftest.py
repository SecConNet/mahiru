"""General support functions and fixtures for the tests.

This is a PyTest special file, see its documentation.
"""
import logging
from pathlib import Path
from unittest.mock import patch
from wsgiref.simple_server import WSGIServer

from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend
import pytest

from proof_of_concept.registry.registry import Registry
from proof_of_concept.rest.registry import RegistryRestApi, RegistryServer

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
    server = RegistryServer(api, ReusingWSGIServer)

    yield

    server.close()


@pytest.fixture
def private_key():
    """Create a private RSA key."""
    return rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend())


@pytest.fixture
def test_dir():
    """Returns the directory the tests are in."""
    return Path(__file__).parent


@pytest.fixture
def temp_path(tmpdir) -> Path:
    """Returns tmpdir as a standard Path object."""
    return Path(str(tmpdir))


@pytest.fixture
def image_dir(temp_path) -> Path:
    """Returns a temp directory for asset store images."""
    path = temp_path / 'store'
    path.mkdir(exist_ok=True)
    return path


@pytest.fixture
def test_image_file(temp_path) -> Path:
    """Returns a path to a mock image file.

    Note: it's just a text file containing 'testing', so the extension
    is a lie.
    """
    test_file = temp_path / 'image.tar.gz'
    with test_file.open('w') as f:
        f.write('testing')
    return test_file
