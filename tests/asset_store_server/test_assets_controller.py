import json
from unittest import mock

from flask_testing import TestCase

from proof_of_concept.asset import Asset
from proof_of_concept.asset_store_server.run_api import create_app

MODULE = 'proof_of_concept.asset_store_server.controllers.assets_controller'


class TestAssetsController(TestCase):
    """AssetsController integration test stubs"""
    def create_app(self):
        return create_app()

    def test_get_asset(self):
        test_asset_id = 'assets/1'
        test_requester = 'test_requester'
        query_string = {'requester': test_requester,
                        'asset_id': test_asset_id}
        db = dict()
        db[test_asset_id] = Asset(id=test_asset_id, data=42)
        with mock.patch.dict(f'{MODULE}.db', db):
            response = self.client.open(
                '/assets',
                method='GET',
                query_string=query_string)
        assert response.status_code == 200, 'Response body is : ' \
                                            + response.data.decode('utf-8')

    def test_get_asset_non_existing_asset(self):
        query_string = {'requester': 'requester_example',
                        'asset_id': 'assets/1'}
        response = self.client.open(
            '/assets',
            method='GET',
            query_string=query_string)
        assert response.status_code == 404, 'Response body is : ' \
                                            + response.data.decode('utf-8')

    def test_store_asset(self):
        """Test case for store_asset

        Store an asset
        """
        asset = Asset(id='assets/1', data=42)
        response = self.client.open(
            '/assets',
            method='POST',
            json=asset.to_dict(),
            content_type='application/json')
        assert response.status_code == 201, 'Response body is : ' + \
                                            response.data.decode('utf-8')

    def test_store_and_retrieve(self):
        test_asset_id = 'assets/1'
        test_requester = 'test_requester'
        asset = Asset(id=test_asset_id, data=42)
        response = self.client.open(
            '/assets',
            method='POST',
            json=asset.to_dict(),
            content_type='application/json')
        assert response.status_code == 201, 'Response body is : ' + \
                                            response.data.decode('utf-8')

        query_string = {'requester': test_requester,
                        'asset_id': test_asset_id}
        response = self.client.open(
            '/assets',
            method='GET',
            query_string=query_string)
        assert response.status_code == 200, 'Response body is : ' \
                                            + response.data.decode('utf-8')
        asset = Asset.from_dict(json.loads(response.json))
        assert asset.id == test_asset_id
