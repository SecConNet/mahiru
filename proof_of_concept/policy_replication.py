"""Classes for distributing policies around the DDM."""
from typing import Dict, Iterable

from proof_of_concept.ddm_client import DDMClient
from proof_of_concept.policy import (
        IPolicySource, InPartyCollection, InAssetCollection, MayAccess,
        ResultOfDataIn, ResultOfComputeIn, Rule)
from proof_of_concept.replication import (
        CanonicalStore, IReplicationServer, ObjectValidator, Replica)


class RuleValidator(ObjectValidator[Rule]):
    """Validates incoming policy rules by checking signatures."""
    def __init__(self, ddm_client: DDMClient, namespace: str) -> None:
        """Create a RuleValidator.

        Checks that rules apply to the given namespace, and that they
        have been signed by the owner of that namespace.

        Args:
            ddm_client: A DDMClient to use.
            namespace: The namespace to expect rules for.
        """
        self._key = ddm_client.get_public_key_for_ns(namespace)
        self._namespace = namespace

    def is_valid(self, rule: Rule) -> bool:
        """Return True iff the rule is properly signed."""
        if isinstance(rule, ResultOfDataIn):
            namespace = rule.data_asset[3:].split('/')[0]
        elif isinstance(rule, ResultOfComputeIn):
            namespace = rule.compute_asset[3:].split('/')[0]
        elif isinstance(rule, MayAccess):
            namespace = rule.asset[3:].split('/')[0]
        elif isinstance(rule, InAssetCollection):
            namespace = rule.asset[3:].split('/')[0]
        elif isinstance(rule, InPartyCollection):
            namespace = rule.collection[3:].split('/')[0]

        if namespace != self._namespace:
            return False
        return rule.has_valid_signature(self._key)


# Defining this where it's used makes mypy crash.
# See https://github.com/python/mypy/issues/7281
_OtherStores = Dict[IReplicationServer[Rule], Replica[Rule]]


class PolicySource(IPolicySource):
    """Ties together various sources of policies."""
    def __init__(
            self, ddm_client: DDMClient, our_store: CanonicalStore[Rule]
            ) -> None:
        """Create a PolicySource.

        This will automatically keep the replicas up-to-date as needed.

        Args:
            ddm_client: A DDMClient to use for getting servers.
            our_store: A store containing our policies.
        """
        self._ddm_client = ddm_client
        self._our_store = our_store
        self._other_stores = dict()     # type: _OtherStores

    def policies(self) -> Iterable[Rule]:
        """Returns the collected rules."""
        self._update()
        our_rules = list(self._our_store.objects())
        their_rules = [
                rule
                for store in self._other_stores.values()
                if store.is_valid()
                for rule in store.objects]
        return our_rules + their_rules

    def _update(self) -> None:
        """Update sources to match the given set."""
        new_servers = self._ddm_client.list_policy_servers()
        # add new servers
        for namespace, new_server in new_servers:
            if new_server not in self._other_stores:
                self._other_stores[new_server] = Replica[Rule](
                        new_server,
                        RuleValidator(self._ddm_client, namespace))

        # removed ones that disappeared
        removed_servers = [
                server for server in self._other_stores
                if server not in list(zip(*new_servers))[1]]  # type: ignore

        for server in removed_servers:
            del(self._other_stores[server])

        # update everyone
        for store in self._other_stores.values():
            store.update()
