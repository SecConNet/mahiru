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
        if asset.kind() not in ('asset', 'asset_collection'):
            raise ValueError(f'Invalid asset kind {asset.kind()}')
        self.asset = asset

        if not isinstance(collection, Identifier):
            collection = Identifier(collection)
        if collection.kind() != 'asset_collection':
            raise ValueError(f'Invalid collection kind {collection.kind()}')
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
        return 'InAssetCollection|{}|{}'.format(
                self.asset, self.collection).encode('utf-8')

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
        if asset.kind() not in ('asset', 'asset_category'):
            raise ValueError(f'Invalid asset kind {asset.kind()}')
        self.asset = asset

        if not isinstance(category, Identifier):
            category = Identifier(category)
        if category.kind() != 'asset_category':
            raise ValueError(f'Invalid category kind {category.kind()}')
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
        return 'InAssetCategory|{}|{}'.format(
                self.asset, self.category).encode('utf-8')

    def signing_namespace(self) -> str:
        """Return the namespace whose owner must sign this rule."""
        return self.category.namespace()


class InPartyCategory(GroupingRule):
    """Says that a PartyCategory contains a Party."""
    def __init__(
            self, party: Union[str, Identifier],
            category: Union[str, Identifier]
            ) -> None:
        """Create an InPartyCategory rule.

        Args:
            party: A party.
            category: The category it is in.
        """
        if not isinstance(party, Identifier):
            party = Identifier(party)
        if party.kind() not in ('party', 'party_category'):
            raise ValueError(f'Invalid party kind {party.kind()}')
        self.party = party

        if not isinstance(category, Identifier):
            category = Identifier(category)
        if category.kind() != 'party_category':
            raise ValueError(f'Invalid category kind {category.kind()}')
        self.category = category

    def grouped(self) -> Identifier:
        """Return the thing being grouped by this rule."""
        return self.party

    def group(self) -> Identifier:
        """Return the grouping of the rule."""
        return self.category

    def __repr__(self) -> str:
        """Return a string representation of this rule."""
        return '("{}" is in "{}")'.format(self.party, self.category)

    def signing_representation(self) -> bytes:
        """Return a string of bytes representing the object.

        This adapts the Signable base class to this class.
        """
        return 'InPartyCategory|{}|{}'.format(
                self.party, self.category).encode('utf-8')

    def signing_namespace(self) -> str:
        """Return the namespace whose owner must sign this rule."""
        return self.category.namespace()


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
        if site.kind() not in ('site', 'site_category'):
            raise ValueError(f'Invalid site kind {site.kind()}')
        self.site = site

        if not isinstance(category, Identifier):
            category = Identifier(category)
        if category.kind() != 'site_category':
            raise ValueError(f'Invalid category kind {site.kind()}')
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
        return 'InSiteCategory|{}|{}'.format(
                self.site, self.category).encode('utf-8')

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
        if not isinstance(site, Identifier):
            site = Identifier(site)
        if site.kind() not in ('site', 'site_category', '*'):
            raise ValueError(f'Invalid site kind {site.kind()}')
        self.site = site

        if not isinstance(asset, Identifier):
            asset = Identifier(asset)
        if asset.kind() not in ('asset', 'asset_collection'):
            raise ValueError(f'Invalid asset kind {asset.kind()}')
        self.asset = asset

    def __repr__(self) -> str:
        """Return a string representation of this rule."""
        return f'("{self.site}" may access "{self.asset}")'

    def signing_representation(self) -> bytes:
        """Return a string of bytes representing the object.

        This adapts the Signable base class to this class.
        """
        return f'MayAccess|{self.site}|{self.asset}'.encode('utf-8')

    def signing_namespace(self) -> str:
        """Return the namespace whose owner must sign this rule."""
        return self.asset.namespace()


class MayUse(Rule):
    """Says that Party party may use Asset asset."""
    def __init__(
            self, party: Union[str, Identifier], asset: Union[str, Identifier],
            conditions: str) -> None:
        """Create a MayUse rule.

        Args:
            party: The party that may access.
            asset: The asset that may be accessed.
            conditions: Conditions under which the asset may be used.
        """
        if not isinstance(party, Identifier):
            party = Identifier(party)
        if party.kind() not in ('party', 'party_category', '*'):
            raise ValueError(f'Invalid party kind {party.kind()}')
        self.party = party

        if not isinstance(asset, Identifier):
            asset = Identifier(asset)
        if asset.kind() not in ('asset', 'asset_collection'):
            raise ValueError(f'Invalid asset kind {asset.kind()}')
        self.asset = asset

        self.conditions = conditions

    def __repr__(self) -> str:
        """Return a string representation of this rule."""
        return f'("{self.party}" may use "{self.asset}")'

    def signing_representation(self) -> bytes:
        """Return a string of bytes representing the object.

        This adapts the Signable base class to this class.
        """
        return 'MayUse|{}|{}|{}'.format(
                self.party, self.asset, self.conditions).encode('utf-8')

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


class ResultOfDataIn(ResultOfIn):
    """ResultOfIn rule on behalf of the data asset owner."""
    def __init__(
            self,
            data_asset: Union[str, Identifier],
            compute_asset: Union[str, Identifier],
            output: str,
            collection: Union[str, Identifier]
            ) -> None:
        """Create a ResultOfDataIn rule.

        Args:
            data_asset: The source data asset.
            compute_asset: The compute asset used to process the data.
            output: The name of the workflow step output that produced
                    the result Asset, or '*' to match any output.
            collection: The output collection.
        """
        super().__init__(data_asset, compute_asset, output, collection)

        if self.data_asset.kind() not in ('asset', 'asset_collection'):
            raise ValueError(
                    f'Invalid data asset kind {self.data_asset.kind()}')

        if self.compute_asset.kind() not in ('asset', 'asset_category', '*'):
            raise ValueError(
                    f'Invalid compute asset kind {self.compute_asset.kind()}')

        if self.collection.kind() != 'asset_collection':
            raise ValueError(
                    f'Invalid collection kind {self.collection.kind()}')

    def signing_namespace(self) -> str:
        """Return the namespace whose owner must sign this rule."""
        return self.data_asset.namespace()

    def signing_representation(self) -> bytes:
        """Return a string of bytes representing the object.

        This adapts the Signable base class to this class.
        """
        return 'ResultOfDataIn|{}|{}|{}|{}'.format(
                self.data_asset, self.compute_asset, self.output,
                self.collection).encode('utf-8')


class ResultOfComputeIn(ResultOfIn):
    """ResultOfIn rule on behalf of the compute asset owner."""
    def __init__(
            self,
            data_asset: Union[str, Identifier],
            compute_asset: Union[str, Identifier],
            output: str,
            collection: Union[str, Identifier]
            ) -> None:
        """Create a ResultOfDataIn rule.

        Args:
            data_asset: The source data asset.
            compute_asset: The compute asset used to process the data.
            output: The name of the workflow step output that produced
                    the result Asset, or '*' to match any output.
            collection: The output collection.
        """
        super().__init__(data_asset, compute_asset, output, collection)

        if self.data_asset.kind() not in ('asset', 'asset_category', '*'):
            raise ValueError(
                    f'Invalid data asset kind {self.data_asset.kind()}')

        if self.compute_asset.kind() not in ('asset', 'asset_collection'):
            raise ValueError(
                    f'Invalid compute asset kind {self.compute_asset.kind()}')

        if self.collection.kind() != 'asset_collection':
            raise ValueError(
                    f'Invalid collection kind {self.collection.kind()}')

    def signing_namespace(self) -> str:
        """Return the namespace whose owner must sign this rule."""
        return self.compute_asset.namespace()

    def signing_representation(self) -> bytes:
        """Return a string of bytes representing the object.

        This adapts the Signable base class to this class.
        """
        return 'ResultOfComputeIn|{}|{}|{}|{}'.format(
                self.data_asset, self.compute_asset, self.output,
                self.collection).encode('utf-8')
