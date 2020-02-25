from typing import List, Set


class Rule:
    """Abstract base class for policy rules.
    """
    pass


class InAssetCollection(Rule):
    """Says that Asset 1 is in AssetCollection 2.
    """
    def __init__(self, asset: str, collection: str) -> None:
        self.asset = asset
        self.collection = collection

    def __repr__(self) -> str:
        return '("{}" is in "{}")'.format(self.asset, self.collection)


class InPartyCollection(Rule):
    """Says that Party party is in PartyCollection collection.
    """
    def __init__(self, party: str, collection: str) -> None:
        self.party = party
        self.collection = collection

    def __repr__(self) -> str:
        return '("{}" is in "{}")'.format(self.party, self.collection)


class MayAccess(Rule):
    """Says that Party party may access Asset asset.
    """
    def __init__(self, party: str, asset: str) -> None:
        self.party = party
        self.asset = asset

    def __repr__(self) -> str:
        return '("{}" may access "{}")'.format(self.party, self.asset)


class ResultOfIn(Rule):
    """Defines collections of results.

    Says that any Asset that was computed from data_asset via
    compute_asset is in collection.
    """
    def __init__(self, data_asset: str, compute_asset: str, collection: str
                 ) -> None:
        self.data_asset = data_asset
        self.compute_asset = compute_asset
        self.collection = collection

    def __repr__(self) -> str:
        return '(result of "{}" on "{}" is in collection "{}")'.format(
                self.compute_asset, self.data_asset, self.collection)


class Permissions:
    """Represents permissions for an asset.
    """
    def __init__(self) -> None:
        """Creates Permissions that do not allow access.
        """
        # friend PolicyManager
        self._sets = list()     # type: List[Set[str]]

    def __str__(self) -> str:
        return 'Permissions({})'.format(self._sets)

    def __repr__(self) -> str:
        return 'Permissions({})'.format(repr(self._sets))

class PolicyManager:
    """Holds a set of rules and can interpret them.
    """
    def __init__(self, policies: List[Rule]) -> None:
        self.policies = policies

    def permissions_for_asset(self, asset: str) -> Permissions:
        """Returns permissions for the given asset.

        This must be a static asset, not an intermediate result, as
        this function only follows InAssetCollection rules.
        """
        result = Permissions()
        result._sets = [self._equivalent_assets(asset)]
        return result

    def propagate_permissions(
            self,
            input_permissions: List[Permissions],
            compute_asset: str
            ) -> Permissions:
        """Determines access for the result of an operation.

        This applies the ResultOfIn rules to determine, from the access
        permissions for the inputs of an operation and the
        compute asset to use, the access permissions of the results.

        Args:
            input_permissions: A list of access permissions to
                    propagate, one for each input.
            compute_asset: The compute asset used in the operation.

        Returns:
            The access permissions of the results.
        """
        result = Permissions()
        for input_perms in input_permissions:
            for asset_set in input_perms._sets:
                rules = self._resultofin_rules(asset_set, compute_asset)
                result._sets.append({
                        asset
                        for rule in rules
                        for asset in self._equivalent_assets(rule.collection)})
        return result

    def may_access(self, permissions: Permissions, party: str) -> bool:
        """Checks whether an asset can be at a site.

        This function checks whether the given site has access rights
        to at least one asset in each of the given set of assets.
        """
        def matches_one(asset_set: Set[str], party: str) -> bool:
            for asset in asset_set:
                for rule in self.policies:
                    if isinstance(rule, MayAccess):
                        if rule.asset == asset and rule.party == party:
                            return True
            return False

        return all([matches_one(asset_set, party)
                    for asset_set in permissions._sets])

    def _equivalent_parties(self, party: str) -> List[str]:
        """Returns all the parties whose rules apply to an asset.

        These are the parties itself, and all parties that are party
        collections that the party is directly or indirectly in.
        """
        cur_parties = list()     # type: List[str]
        new_parties = [party]
        while new_parties:
            cur_parties.extend(new_parties)
            new_parties = list()
            for party in cur_parties:
                for rule in self.policies:
                    if isinstance(rule, InPartyCollection):
                        if rule.party == party:
                            new_parties.append(rule.collection)
        return cur_parties

    def _equivalent_assets(self, asset: str) -> Set[str]:
        """Returns all the assets whose rules apply to an asset.

        These are the asset itself, and all assets that are asset
        collections that the asset is directly or indirectly in.
        """
        cur_assets = set()     # type: Set[str]
        new_assets = {asset}
        while new_assets:
            cur_assets |= new_assets
            new_assets = set()
            for asset in cur_assets:
                for rule in self.policies:
                    if isinstance(rule, InAssetCollection):
                        if rule.asset == asset:
                            if rule.collection not in cur_assets:
                                new_assets.add(rule.collection)
        cur_assets.add('*')
        return cur_assets

    def _resultofin_rules(
            self, asset_set: Set[str], compute_asset: str
            ) -> List[ResultOfIn]:
        """Returns all ResultOfIn rules that apply to these assets.

        These are rules that have one of the given assets in their
        asset field, and the given compute_asset or an equivalent one.
        """
        def rules_for_asset(asset: str) -> List[ResultOfIn]:
            """Gets all matching rules for the given single asset.
            """
            result = list()     # type: List[ResultOfIn]
            assets = self._equivalent_assets(asset)
            for rule in self.policies:
                if isinstance(rule, ResultOfIn):
                    for asset in assets:
                        if rule.data_asset == asset:
                            result.append(rule)
            return result

        comp_assets = self._equivalent_assets(compute_asset)

        rules = list()  # type: List[ResultOfIn]
        for asset in asset_set:
            new_rules = rules_for_asset(asset)
            rules.extend([rule
                          for rule in new_rules
                          if rule.compute_asset in comp_assets])
        return rules
