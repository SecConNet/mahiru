from typing import Any, Dict, Optional, Tuple

from definitions import IAssetStore
from workflow import Workflow


class AssetStore(IAssetStore):
    """A simple store for assets.
    """
    def __init__(self, name: str) -> None:
        """Create a new empty AssetStore.
        """
        self.name = name
        self._assets = dict()   # type: Dict[str, Any]
        self._provenance = dict()   # type: Dict[str, Optional[Workflow]]

    def __repr__(self) -> str:
        return 'AssetStore({})'.format(self.name)

    def store(
            self, name: str, data: Any, provenance: Optional[Workflow] = None
            ) -> None:
        """Stores an asset.

        Args:
            name: Name to store asset under.
            data: Asset data to store.
            provenance: Workflow that generated this asset.

        Raises:
            KeyError: If there's already an asset with name ``name``.
        """
        if name in self._assets:
            raise KeyError('There is already an asset with that name')
        self._assets[name] = data
        self._provenance[name] = provenance

    def retrieve(self, name: str) -> Tuple[Any, Optional[Workflow]]:
        """Retrieves an asset.

        Args:
            name: Name of the asset to retrieve.

        Return:
            The asset data stored under the given name, and its
            provenance.

        Raises:
            KeyError: If no asset with the given name is stored here.
        """
        print('{} servicing request for data {}, '.format(self, name), end='')
        try:
            data = self._assets[name]
            provenance = self._provenance[name]
            print('sending...')
            return data, provenance
        except KeyError:
            print('not found.')
            raise
