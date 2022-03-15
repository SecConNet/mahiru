"""Classes representing rules."""
from typing import Union

from mahiru.definitions.identifier import Identifier
from mahiru.definitions.policy import Rule


class GroupingRule(Rule):
    """Subclass for rules that represent groupings."""
    def grouped(self) -> Identifier:
        """Return the thing being grouped by this rule."""
        raise NotImplementedError

    def group(self) -> Identifier:
        """Return the grouping of the rule.

        This returns the collection or category.
        """
        raise NotImplementedError


class InAssetCollection(GroupingRule):
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

    def grouped(self) -> Identifier:
        """Return the thing being grouped by this rule."""
        return self.asset

    def group(self) -> Identifier:
        """Return the grouping of the rule."""
        return self.collection

    def signing_representation(self) -> bytes:
        """Return a string of bytes representing the object.

        This adapts the Signable base class to this class.
        """
        return '{}|{}'.format(self.asset, self.collection).encode('utf-8')

    def signing_namespace(self) -> str:
        """Return the namespace whose owner must sign this rule."""
        return self.asset.namespace()


class InAssetCategory(GroupingRule):
    """Says that an AssetCategory contains an Asset."""
    def __init__(
            self, asset: Union[str, Identifier],
            category: Union[str, Identifier]
            ) -> None:
        """Create an InAssetCategory rule.

        Args:
            asset: The asset to add to the category.
            category: The category to add it to.
        """
        if not isinstance(asset, Identifier):
            asset = Identifier(asset)
        self.asset = asset
        if not isinstance(category, Identifier):
            category = Identifier(category)
        self.category = category

    def grouped(self) -> Identifier:
        """Return the thing being grouped by this rule."""
        return self.asset

    def group(self) -> Identifier:
        """Return the grouping of the rule."""
        return self.category

    def __repr__(self) -> str:
        """Return a string representation of this rule."""
        return '("{}" is in "{}")'.format(self.asset, self.category)

    def signing_representation(self) -> bytes:
        """Return a string of bytes representing the object.

        This adapts the Signable base class to this class.
        """
        return '{}|{}'.format(self.asset, self.category).encode('utf-8')

    def signing_namespace(self) -> str:
        """Return the namespace whose owner must sign this rule."""
        return self.category.namespace()


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


class InSiteCategory(GroupingRule):
    """Says that a SiteCategory contains an Site."""
    def __init__(
            self, site: Union[str, Identifier],
            category: Union[str, Identifier]
            ) -> None:
        """Create an InSiteCategory rule.

        Args:
            site: The site to add to the category.
            category: The category to add it to.
        """
        if not isinstance(site, Identifier):
            site = Identifier(site)
        self.site = site
        if not isinstance(category, Identifier):
            category = Identifier(category)
        self.category = category

    def grouped(self) -> Identifier:
        """Return the thing being grouped by this rule."""
        return self.site

    def group(self) -> Identifier:
        """Return the grouping of the rule."""
        return self.category

    def __repr__(self) -> str:
        """Return a string representation of this rule."""
        return '("{}" is in "{}")'.format(self.site, self.category)

    def signing_representation(self) -> bytes:
        """Return a string of bytes representing the object.

        This adapts the Signable base class to this class.
        """
        return '{}|{}'.format(self.site, self.category).encode('utf-8')

    def signing_namespace(self) -> str:
        """Return the namespace whose owner must sign this rule."""
        return self.category.namespace()


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
            output: str,
            collection: Union[str, Identifier]
            ) -> None:
        """Create a ResultOfIn rule.

        Args:
            data_asset: The source data asset.
            compute_asset: The compute asset used to process the data.
            output: The name of the workflow step output that produced
                    the result Asset, or '*' to match any output.
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
        self.output = output
        self.collection = collection

    def __repr__(self) -> str:
        """Return a string representation of this rule."""
        return '(result "{}" of "{}" on "{}" is in collection "{}")'.format(
                self.output, self.compute_asset, self.data_asset,
                self.collection)

    def signing_representation(self) -> bytes:
        """Return a string of bytes representing the object.

        This adapts the Signable base class to this class.
        """
        return '{}|{}|{}|{}'.format(
                self.data_asset, self.compute_asset, self.output,
                self.collection).encode('utf-8')


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
