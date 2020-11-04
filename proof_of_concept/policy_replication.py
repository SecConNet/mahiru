"""Classes for distributing policies around the DDM."""
from typing import Dict, Iterable, List

from proof_of_concept.ddm_client import DDMClient
from proof_of_concept.definitions import PolicyUpdate
from proof_of_concept.policy import (
        IPolicySource, InPartyCollection, InAssetCollection, MayAccess,
        ResultOfDataIn, ResultOfComputeIn, Rule)
from proof_of_concept.replication import (
        CanonicalStore, IReplicationSource, ObjectValidator, Replica,
        ReplicationServer)


# Defining this where it's used makes mypy crash.
# See https://github.com/python/mypy/issues/7281
_OtherStores = Dict[IReplicationSource[Rule], Replica[Rule]]


class PolicySource(IPolicySource):
    """Ties together various sources of policies."""
    def __init__(self, ddm_client: DDMClient) -> None:
        """Create a PolicySource.

        This will automatically keep the replicas up-to-date as needed.

        Args:
            ddm_client: A DDMClient to use for getting servers.
        """
        self._ddm_client = ddm_client

    def policies(self) -> Iterable[Rule]:
        """Returns the collected rules."""
        rules = list()      # type: List[Rule]
        for site_rules in self._ddm_client.get_rules().values():
            rules.extend(site_rules)
        return rules


class PolicyServer(ReplicationServer[Rule]):
    """A replication server for policy rules."""
    UpdateType = PolicyUpdate
