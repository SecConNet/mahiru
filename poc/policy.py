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


class PolicyManager:
    """Holds a set of rules and can interpret them.
    """
    def __init__(self, policies: List[Rule]) -> None:
        self.policies = policies

    def may_access(self, asset_colls: List[Set[str]], party: str) -> bool:
        """Checks whether an asset can be at a site.

        This function checks whether the given site has access rights
        to at least one asset in each of the given set of assets.
        """
        def matches_one(asset_coll: Set[str], party: str) -> bool:
            for asset in asset_coll:
                for rule in self.policies:
                    if isinstance(rule, MayAccess):
                        if rule.asset == asset and rule.party == party:
                            return True
            return False

        return all([matches_one(asset_coll, party)
                    for asset_coll in asset_colls])

    def equivalent_assets(self, asset: str) -> Set[str]:
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

    def resultofin_rules(
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
            assets = self.equivalent_assets(asset)
            for rule in self.policies:
                if isinstance(rule, ResultOfIn):
                    for asset in assets:
                        if rule.data_asset == asset:
                            result.append(rule)
            return result

        comp_assets = self.equivalent_assets(compute_asset)

        rules = list()  # type: List[ResultOfIn]
        for asset in asset_set:
            new_rules = rules_for_asset(asset)
            rules.extend([rule
                          for rule in new_rules
                          if rule.compute_asset in comp_assets])
        return rules
