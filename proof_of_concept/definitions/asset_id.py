"""Identifier for assets."""
from typing import Any, cast, Type


class AssetId(str):
    """An ID of an asset."""
    def __new__(cls: Type['AssetId'], seq: Any) -> 'AssetId':
        """Create an AssetId.

        Args:
            seq: Contents, will be converted to a string using str(),
            then used as the id.

        Raises:
            ValueError: If str(seq) is not a valid ID.
        """
        data = str(seq)
        if data.startswith('id:'):
            parts = len(data.split(':'))
            if parts < 3 or parts > 4:
                raise ValueError(f'Invalid asset id: {data}')
        elif data.startswith('hash:'):
            if not all([c in '0123456789abcdef' for c in data[5:]]):
                raise ValueError(f'Invalid asset id: {data}')
        elif data == '*':
            # Valid as a wildcard in rules
            pass
        else:
            raise ValueError(f'Invalid asset id type: {data}')

        return str.__new__(cls, seq)        # type: ignore

    @staticmethod
    def from_key(key: str) -> 'AssetId':
        """Creates an AssetId from a key (hash).

        Args:
            key: A hash of a workflow that created this asset.

        Returns:
            The AssetId for the workflow result.
        """
        return AssetId(f'hash:{key}')

    def is_primary(self) -> bool:
        """Returns whether this is a primary asset."""
        return self.startswith('id:')

    def is_concrete(self) -> bool:
        """Returns whether this is a concrete asset.

        An asset is concrete if it is primary and has a site it can
        be downloaded from.
        """
        return self.is_primary() and len(self.split(':')) == 4

    def namespace(self) -> str:
        """Returns the namespace this asset is in.

        Returns:
            The namespace.

        Raises:
            RuntimeError: If this is not a primary asset.
        """
        if not self.is_primary():
            raise RuntimeError('Namespace of secondary asset requested')
        return self.split(':')[1]

    def location(self) -> str:
        """Returns the name of the site storing this asset.

        Returns:
            A site name.

        Raises:
            RuntimeError: If this is not a concrete asset.
        """
        if not self.is_concrete():
            raise RuntimeError(
                    'Location requested of non-concrete asset {self.data}')
        return self.split(':')[3]
