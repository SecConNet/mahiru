"""Asset stores store data and compute assets."""
from typing import Dict

from proof_of_concept.asset import Asset
from proof_of_concept.definitions import IAssetStore
from proof_of_concept.permission_calculator import PermissionCalculator
from proof_of_concept.policy import PolicyEvaluator


class AssetStore(IAssetStore):
    """A simple store for assets."""
    def __init__(self, name: str) -> None:
        """Create a new empty AssetStore."""
        self.name = name
        # self._policy_evaluator = policy_evaluator
        # self._permission_calculator = PermissionCalculator(policy_evaluator)
        self._assets = dict()  # type: Dict[str, Asset]

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        return 'AssetStore({})'.format(self.name)

    def store(self, asset: Asset) -> None:
        """Stores an asset.

        Args:
            asset: asset object to store

        Raises:
            KeyError: If there's already an asset with the asset id.

        """
        if asset.id in self._assets:
            raise KeyError(f'There is already an asset with id {id}')

        self._assets[asset.id] = asset

    def retrieve(self, asset_id: str, requester: str
                 ) -> Asset:
        """Retrieves an asset.

        Args:
            asset_id: ID of the asset to retrieve.
            requester: Name of the party making the request.

        Return:
            The asset object with asset_id.

        Raises:
            KeyError: If no asset with the given id is stored here.

        """
        print(
                '{} servicing request from {} for data {}, '.format(
                    self, requester, asset_id),
                end='')
        asset = self._assets[asset_id]
        return asset
        # try:
        #     asset = self._assets[asset_id]
        #     perms = self._permission_calculator.calculate_permissions(
        #             asset.metadata.job)
        #     perm = perms[asset.metadata.item]
        #     if not self._policy_evaluator.may_access(perm, requester):
        #         raise RuntimeError('Security error, access denied')
        #     print('sending...')
        #     return asset
        # except KeyError:
        #     print('not found.')
        #     raise
