"""General support functions and fixtures for the tests.

This is a PyTest special file, see its documentation.
"""
import logging
from pathlib import Path
from unittest.mock import patch
from wsgiref.simple_server import WSGIServer

from cryptography import x509
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import load_pem_private_key
import pytest

from mahiru.components.registry_client import RegistryClient
from mahiru.registry.registry import Registry
from mahiru.rest.registry import RegistryRestApi, RegistryServer
from mahiru.rest.registry_client import (
        RegistrationRestClient, RegistryRestClient)


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
def registry_client():
    """Create a registry REST API replication client.

    Connects to the default service endpoint.

    """
    registry_rest_client = RegistryRestClient()
    return RegistryClient(registry_rest_client)


@pytest.fixture
def registration_client():
    """Create a registration REST API registration client.

    Connects to the default service endpoint.

    """
    return RegistrationRestClient()


@pytest.fixture
def certs_dir():
    return Path(__file__).parents[1] / 'build' / 'certs'


@pytest.fixture
def party1_main_certificate(certs_dir):
    """Load certificate from disk.

    Use 'make certificates' to generate the file.
    """
    cert_file = certs_dir / 'site1' / 'certs' / 'party1_main_cert.pem'
    with cert_file.open('rb') as f:
        return x509.load_pem_x509_certificate(f.read())


@pytest.fixture
def party2_main_certificate(certs_dir):
    """Load certificate from disk.

    Use 'make certificates' to generate the file.
    """
    cert_file = certs_dir / 'site2' / 'certs' / 'party2_main_cert.pem'
    with cert_file.open('rb') as f:
        return x509.load_pem_x509_certificate(f.read())


@pytest.fixture
def party3_main_certificate(certs_dir):
    """Load certificate from disk.

    Use 'make certificates' to generate the file.
    """
    cert_file = certs_dir / 'site3' / 'certs' / 'party3_main_cert.pem'
    with cert_file.open('rb') as f:
        return x509.load_pem_x509_certificate(f.read())


@pytest.fixture
def party1_main_key(certs_dir):
    """Load the main private key."""
    key_file = certs_dir / 'site1' / 'private' / 'party1_main_key.pem'
    with key_file.open('rb') as f:
        return load_pem_private_key(f.read(), None)


@pytest.fixture
def party2_main_key(certs_dir):
    """Load the main private key."""
    key_file = certs_dir / 'site2' / 'private' / 'party2_main_key.pem'
    with key_file.open('rb') as f:
        return load_pem_private_key(f.read(), None)


@pytest.fixture
def party3_main_key(certs_dir):
    """Load the main private key."""
    key_file = certs_dir / 'site3' / 'private' / 'party3_main_key.pem'
    with key_file.open('rb') as f:
        return load_pem_private_key(f.read(), None)


@pytest.fixture
def site1_https_certificate(certs_dir):
    """Load certificate from disk.

    Use 'make certificates' to generate the file.
    """
    cert_file = certs_dir / 'site1' / 'certs' / 'site1_https_cert.pem'
    with cert_file.open('rb') as f:
        return x509.load_pem_x509_certificate(f.read())


@pytest.fixture
def site2_https_certificate(certs_dir):
    """Load certificate from disk.

    Use 'make certificates' to generate the file.
    """
    cert_file = certs_dir / 'site2' / 'certs' / 'site2_https_cert.pem'
    with cert_file.open('rb') as f:
        return x509.load_pem_x509_certificate(f.read())


@pytest.fixture
def site3_https_certificate(certs_dir):
    """Load certificate from disk.

    Use 'make certificates' to generate the file.
    """
    cert_file = certs_dir / 'site3' / 'certs' / 'site3_https_cert.pem'
    with cert_file.open('rb') as f:
        return x509.load_pem_x509_certificate(f.read())


@pytest.fixture
def party1_user_ca_certificate(certs_dir):
    """Load certificate from disk.

    Use 'make certificates' to generate the file.
    """
    cert_file = certs_dir / 'site1' / 'certs' / 'party1_user_ca_cert.pem'
    with cert_file.open('rb') as f:
        return x509.load_pem_x509_certificate(f.read())


@pytest.fixture
def party2_user_ca_certificate(certs_dir):
    """Load certificate from disk.

    Use 'make certificates' to generate the file.
    """
    cert_file = certs_dir / 'site2' / 'certs' / 'party2_user_ca_cert.pem'
    with cert_file.open('rb') as f:
        return x509.load_pem_x509_certificate(f.read())


@pytest.fixture
def party3_user_ca_certificate(certs_dir):
    """Load certificate from disk.

    Use 'make certificates' to generate the file.
    """
    cert_file = certs_dir / 'site3' / 'certs' / 'party3_user_ca_cert.pem'
    with cert_file.open('rb') as f:
        return x509.load_pem_x509_certificate(f.read())


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
