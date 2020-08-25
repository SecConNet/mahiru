# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from swagger_server.models.asset import Asset  # noqa: E501
from swagger_server.test import BaseTestCase


class TestAssetsController(BaseTestCase):
    """AssetsController integration test stubs"""

    def test_get_asset(self):
        """Test case for get_asset

        Retrieve an asset by ID
        """
        query_string = [('requester', 'requester_example')]
        response = self.client.open(
            '/assets/{asset_id}'.format(asset_id='asset_id_example'),
            method='GET',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_store_asset(self):
        """Test case for store_asset

        Store an asset
        """
        body = Asset()
        response = self.client.open(
            '/assets',
            method='POST',
            data=json.dumps(body),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
