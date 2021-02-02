from datetime import datetime
import logging
from unittest.mock import MagicMock
from threading import Thread
from wsgiref.simple_server import WSGIRequestHandler

from falcon import App
import pytest

from proof_of_concept.components.asset_store import AssetStore
from proof_of_concept.definitions.assets import DataAsset
from proof_of_concept.definitions.identifier import Identifier
from proof_of_concept.definitions.registry import RegisteredObject
from proof_of_concept.replication import ReplicaUpdate
from proof_of_concept.rest.client import SiteRestClient
from proof_of_concept.rest.ddm_site import (
        AssetImageAccessHandler, ThreadingWSGIServer)


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
    asset_image_access = AssetImageAccessHandler(asset_store)
    app.add_route('/assets/{asset_id}/image', asset_image_access)
    server = ThreadingWSGIServer(('0.0.0.0', 0), WSGIRequestHandler)
    server.set_app(app)

    thread = Thread(
            target=server.serve_forever,
            name='TestServer')
    thread.start()

    yield f'http://{server.server_name}:{server.server_port}'

    server.shutdown()
    server.server_close()
    thread.join()


def test_asset_download(temp_path, asset_id, image_server, caplog):
    caplog.set_level(logging.DEBUG)
    registry_client = MagicMock()
    empty_ddm = ReplicaUpdate[RegisteredObject](
            0, 0, datetime(2100, 1, 1, 0, 0, 0), set(), set())
    registry_client.get_updates_since = lambda: empty_ddm
    client = SiteRestClient('site:ns:site', MagicMock(), registry_client)

    download_path = temp_path / 'retrieved_image.tar.gz'
    client.retrieve_asset_image(
            f'{image_server}/assets/{asset_id}/image', download_path)

    with download_path.open('r') as f:
        image_data = f.read()

    assert image_data == 'testing'
