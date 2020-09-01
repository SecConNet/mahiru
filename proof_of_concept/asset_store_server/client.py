"""Client for asset store server."""
import json
import logging
from time import sleep

import requests

from proof_of_concept.asset import Asset
from proof_of_concept.asset_store_server.run_api import run_async
from proof_of_concept.definitions import IAssetStore
from proof_of_concept.permission_calculator import PermissionCalculator
from proof_of_concept.policy import PolicyEvaluator

logger = logging.getLogger(__file__)


class AssetStoreClient(IAssetStore):
    def __init__(self, name: str, policy_evaluator: PolicyEvaluator,
                 host: str = 'http://127.0.0.1', port: int = 5000) -> None:
        """Create a new AssetStoreClient."""
        self.name = name
        self._policy_evaluator = policy_evaluator
        self._permission_calculator = PermissionCalculator(policy_evaluator)
        self._port = port
        self._api_url = f'{host}:{port}'
        self._setup_server()

    def _setup_server(self) -> None:
        logger.info(f'Starting asset store server {self.name} '
                    f'on port: {self._port}')
        run_async(port=self._port)
        sleep(0.2)

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        return f'AssetStoreClient({self.name}, {self._port})'

    def store(self, asset: Asset) -> None:
        """Stores an asset.

        Args:
            asset: asset object to store
        """
        result = requests.post(url=f'{self._api_url}/assets',
                               json=asset.to_dict())
        result.raise_for_status()

    def retrieve(self, asset_id: str, requester: str) -> Asset:
        """Retrieves an asset.

        Args:
            asset_id: ID of the asset to retrieve.
            requester: Name of the party making the request.

        Return:
            The asset object with asset_id.

        TODO: We now solve permissions on the client side, which of course
            defeats the purpose of having a REST asset store server. We
            should probably also RESTify the policy evaluator and call that
            from within the asset store server.
        """
        logger.info(f'{self}: servicing request from {requester} for data: '
                    f'{asset_id}')
        asset = self._get_asset(asset_id)
        perms = self._permission_calculator.calculate_permissions(
            asset.metadata.job)
        perm = perms[asset.metadata.item]
        if not self._policy_evaluator.may_access(perm, requester):
            raise RuntimeError(f'{self}: Security error, access denied'
                               f'for {requester} to {asset_id}')
        return asset

    def _get_asset(self, asset_id: str) -> Asset:
        """Perform a GET request to obtain the asset with asset_id."""
        result = requests.get(url=f'{self._api_url}/assets',
                              params={'requester': 'ignored_requester',
                                      'asset_id': asset_id})
        if result.status_code == 404:
            raise KeyError(f'{self}: Asset {asset_id} not found.')
        result.raise_for_status()
        return Asset.from_dict(json.loads(result.json()))
