from unittest import mock

from flask_testing import TestCase

from proof_of_concept.asset import Asset
from proof_of_concept.asset_store_server.__main__ import create_app


class TestAssetsController(TestCase):
    """AssetsController integration test stubs"""
    store = mock.MagicMock()

    def create_app(self):
        return create_app(store=self.store)

    def test_get_asset(self):
        query_string = [('requester', 'requester_example')]
        self.store.retrieve.return_value = Asset(id='asset1', data=42)

        response = self.client.open(
            '/assets/asset_id',
            method='GET',
            query_string=query_string)
        assert response.status_code == 200, 'Response body is : ' \
                                            + response.data.decode('utf-8')

    def test_get_asset_non_existing_asset(self):
        query_string = [('requester', 'requester_example')]
        self.store.retrieve.side_effect = KeyError
        response = self.client.open(
            '/assets/asset_id',
            method='GET',
            query_string=query_string)
        assert response.status_code == 404, 'Response body is : ' \
                                            + response.data.decode('utf-8')

    def test_store_asset(self):
        """Test case for store_asset

        Store an asset
        """
        asset = Asset(id='asset1', data=42)
        response = self.client.open(
            '/assets',
            method='POST',
            json=asset.to_dict(),
            content_type='application/json')
        assert response.status_code == 201, 'Response body is : ' + \
                                            response.data.decode('utf-8')
