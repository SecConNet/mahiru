from typing import Any, Dict

from definitions import IAssetStore


class AssetStore(IAssetStore):
    """A simple store for assets.
    """
    def __init__(self, name: str) -> None:
        """Create a new empty AssetStore.
        """
        self.name = name
        self._assets = dict()   # type: Dict[str, Any]

    def __repr__(self) -> str:
        return 'AssetStore({})'.format(self.name)

    def store(self, name: str, data: Any) -> None:
        """Stores an asset.

        Args:
            name: Name to store asset under.
            data: Asset data to store.

        Raises:
            KeyError: If there's already an asset with name ``name``.
        """
        if name in self._assets:
            raise KeyError('There is already an asset with that name')
        self._assets[name] = data

    def retrieve(self, name: str) -> Any:
        """Retrieves an asset.

        Args:
            name: Name of the asset to retrieve.

        Return:
            The asset data stored under the given name.

        Raises:
            KeyError: If no asset with the given name is stored here.
        """
        print('{} servicing request for data {}, '.format(self, name), end='')
        try:
            data = self._assets[name]
            print('sending...')
            return data
        except KeyError:
            print('not found.')
            raise
