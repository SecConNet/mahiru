"""Asset stores store data and compute assets."""
import logging
from typing import Dict

from proof_of_concept.asset import Asset

logger = logging.getLogger(__file__)


class AssetStore:
    """A simple store for assets."""
    def __init__(self, name: str) -> None:
        """Create a new empty AssetStore."""
        self.name = name
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
        try:
            asset = self._assets[asset_id]
            return asset
        except KeyError:
            logger.info(f'{self}: Asset {asset_id} not found'
                        f'(requester = {requester}).')
            raise
