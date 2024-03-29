from datetime import datetime
import logging
from unittest.mock import MagicMock
from threading import Thread
from wsgiref.simple_server import WSGIRequestHandler

from falcon import App
import pytest
import requests

from mahiru.components.asset_store import AssetStore
from mahiru.definitions.assets import DataAsset
from mahiru.definitions.identifier import Identifier
from mahiru.definitions.registry import RegisteredObject
from mahiru.replication import ReplicaUpdate
from mahiru.rest.site_client import SiteRestClient
from mahiru.rest.ddm_site import AssetImageAccessHandler, ThreadingWSGIServer


@pytest.fixture
def asset_id():
    return Identifier('asset:ns:test_asset:ns:site')


@pytest.fixture
def asset_store(temp_path, test_image_file, asset_id):
    asset_store = AssetStore(MagicMock(), temp_path)
    asset = DataAsset(asset_id, None, str(test_image_file))
    asset_store.store(asset)
    return asset_store


@pytest.fixture
def image_server(asset_store):
    app = App()
    asset_image_access = AssetImageAccessHandler(
            MagicMock(), asset_store)
    app.add_route('/assets/{asset_id}/image', asset_image_access)
    server = ThreadingWSGIServer(('0.0.0.0', 0), WSGIRequestHandler)
    server.set_app(app)

    thread = Thread(
            target=server.serve_forever,
            name='TestServer')
    thread.start()

    server_address = f'http://{server.server_name}:{server.server_port}'

    # wait for server to come up
    requests.get(server_address, timeout=(600.0, 1.0))

    yield server_address

    server.shutdown()
    server.server_close()
    thread.join()


@pytest.fixture
def mock_empty_registry_client():
    registry_client = MagicMock()
    empty_ddm = ReplicaUpdate[RegisteredObject](
            0, 0, datetime(2100, 1, 1, 0, 0, 0), set(), set())
    registry_client.get_updates_since = lambda: empty_ddm
    return registry_client


def test_asset_download(
        temp_path, asset_id, image_server, mock_empty_registry_client):

    client = SiteRestClient('site:ns:site', mock_empty_registry_client)

    download_path = temp_path / 'retrieved_image.tar.gz'
    client.retrieve_asset_image(
            f'{image_server}/assets/{asset_id}/image', download_path)

    with download_path.open('r') as f:
        image_data = f.read()

    assert image_data == 'testing'
