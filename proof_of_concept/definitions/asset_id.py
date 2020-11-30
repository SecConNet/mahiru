"""Identifier for assets."""
from typing import Any, cast, Type


class AssetId(str):
    """An id of an asset.

    An Asset id is a string of any of the following forms:

    1. id:<namespace>:<name>:<site>
    2. id:<namespace>:<name>
    3. hash:<hash>

    Form 1 is used for data assets and compute assets, i.e. data sets
    and software wrapped in container images. These are primary assets,
    meaning they are put into the DDM from the outside, and they are
    concrete assets, meaning there's an actual object (the container
    image) associated with them, which is stored at <site>.

    Form 2 is used for asset collections. These are named (with an
    AssetId) collections of assets created implicitly by
    InAssetCollection rules. These are also considered primary assets,
    but they are abstract, not concrete, because there is no
    downloadable object associated with them.

    Form 3 is used for results of data processing done in the DDM. The
    hash identifying the object is derived from the workflow used to
    create this asset. These objects are called secondary assets, as
    they are temporary, only existing during workflow execution. Their
    id does not specify their location.

    """
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
            n_parts = len(data.split(':'))
            if n_parts not in (3, 4):
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

    @classmethod
    def from_id_hash(cls, id_hash: str) -> 'AssetId':
        """Creates an AssetId from an id hash.

        Args:
            id_hash: A hash of a workflow that created this asset.

        Returns:
            The AssetId for the workflow result.
        """
        return cls(f'hash:{id_hash}')

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
