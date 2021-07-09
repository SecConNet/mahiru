"""Classes representing rules."""
from typing import Union

from mahiru.definitions.identifier import Identifier
from mahiru.definitions.policy import Rule


class InAssetCollection(Rule):
    """Says that an Asset is in an AssetCollection.

    This implies that anyone who can access the collection can access
    the Asset.
    """
    def __init__(
            self, asset: Union[str, Identifier],
            collection: Union[str, Identifier]
            ) -> None:
        """Create an InAssetCollection rule.

        Args:
            asset: The asset to put into the collection.
            collection: The collection to put it into.
        """
        if not isinstance(asset, Identifier):
            asset = Identifier(asset)
        self.asset = asset
        if not isinstance(collection, Identifier):
            collection = Identifier(collection)
        self.collection = collection

    def __repr__(self) -> str:
        """Return a string representation of this rule."""
        return '("{}" is in "{}")'.format(self.asset, self.collection)

    def signing_representation(self) -> bytes:
        """Return a string of bytes representing the object.

        This adapts the Signable base class to this class.
        """
        return '{}|{}'.format(self.asset, self.collection).encode('utf-8')

    def signing_namespace(self) -> str:
        """Return the namespace whose owner must sign this rule."""
        return self.asset.namespace()


class InPartyCollection(Rule):
    """Says that Party party is in PartyCollection collection."""
    def __init__(
            self, party: Union[str, Identifier],
            collection: Union[str, Identifier]
            ) -> None:
        """Create an InPartyCollection rule.

        Args:
            party: A party.
            collection: The collection it is in.
        """
        if not isinstance(party, Identifier):
            party = Identifier(party)
        self.party = party
        if not isinstance(collection, Identifier):
            collection = Identifier(collection)
        self.collection = collection

    def __repr__(self) -> str:
        """Return a string representation of this rule."""
        return '("{}" is in "{}")'.format(self.party, self.collection)

    def signing_representation(self) -> bytes:
        """Return a string of bytes representing the object.

        This adapts the Signable base class to this class.
        """
        return '{}|{}'.format(self.party, self.collection).encode('utf-8')

    def signing_namespace(self) -> str:
        """Return the namespace whose owner must sign this rule."""
        return self.collection.namespace()


class MayAccess(Rule):
    """Says that Site site may access Asset asset."""
    def __init__(
            self, site: Union[str, Identifier], asset: Union[str, Identifier]
            ) -> None:
        """Create a MayAccess rule.

        Args:
            site: The site that may access.
            asset: The asset that may be accessed.
        """
        self.site = site if isinstance(site, Identifier) else Identifier(site)
        if not isinstance(asset, Identifier):
            asset = Identifier(asset)
        self.asset = asset

    def __repr__(self) -> str:
        """Return a string representation of this rule."""
        return f'("{self.site}" may access "{self.asset}")'

    def signing_representation(self) -> bytes:
        """Return a string of bytes representing the object.

        This adapts the Signable base class to this class.
        """
        return f'{self.site}|{self.asset}'.encode('utf-8')

    def signing_namespace(self) -> str:
        """Return the namespace whose owner must sign this rule."""
        return self.asset.namespace()


class ResultOfIn(Rule):
    """Defines collections of results.

    Says that any Asset that was computed from data_asset via
    compute_asset is in collection, according to either the owner of
    data_asset or the owner of compute_asset.
    """
    def __init__(
            self,
            data_asset: Union[str, Identifier],
            compute_asset: Union[str, Identifier],
            collection: Union[str, Identifier]
            ) -> None:
        """Create a ResultOfIn rule.

        Args:
            data_asset: The source data asset.
            compute_asset: The compute asset used to process the data.
            collection: The output collection.
        """
        if not isinstance(data_asset, Identifier):
            data_asset = Identifier(data_asset)

        if not isinstance(compute_asset, Identifier):
            compute_asset = Identifier(compute_asset)

        if not isinstance(collection, Identifier):
            collection = Identifier(collection)

        self.data_asset = data_asset
        self.compute_asset = compute_asset
        self.collection = collection

    def __repr__(self) -> str:
        """Return a string representation of this rule."""
        return '(result of "{}" on "{}" is in collection "{}")'.format(
                self.compute_asset, self.data_asset, self.collection)

    def signing_representation(self) -> bytes:
        """Return a string of bytes representing the object.

        This adapts the Signable base class to this class.
        """
        return '{}|{}|{}'.format(
                self.data_asset, self.compute_asset, self.collection
                ).encode('utf-8')


class ResultOfDataIn(ResultOfIn):
    """ResultOfIn rule on behalf of the data asset owner."""
    def signing_namespace(self) -> str:
        """Return the namespace whose owner must sign this rule."""
        return self.data_asset.namespace()


class ResultOfComputeIn(ResultOfIn):
    """ResultOfIn rule on behalf of the compute asset owner."""
    def signing_namespace(self) -> str:
        """Return the namespace whose owner must sign this rule."""
        return self.compute_asset.namespace()
