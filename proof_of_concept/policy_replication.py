"""Classes for distributing policies around the DDM."""
from typing import Dict, Iterable, List, Set

from proof_of_concept.ddm_client import RegistryClient
from proof_of_concept.definitions import (
        PolicyUpdate, RegisteredObject, SiteDescription)
from proof_of_concept.policy import (
        IPolicyCollection, InPartyCollection, InAssetCollection, MayAccess,
        ResultOfDataIn, ResultOfComputeIn, Rule)
from proof_of_concept.replication import (
        CanonicalStore, ObjectValidator, Replica)
from proof_of_concept.replication_rest import ReplicationClient
from proof_of_concept.validation import Validator

from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey


class PolicyClient(ReplicationClient[Rule]):
    """A client for policy servers."""
    UpdateType = PolicyUpdate


class RuleValidator(ObjectValidator[Rule]):
    """Validates incoming policy rules by checking signatures."""
    def __init__(self, namespace: str, key: RSAPublicKey) -> None:
        """Create a RuleValidator.

        Checks that rules apply to the given namespace, and that they
        have been signed by the owner of that namespace.

        Args:
            namespace: The namespace to expect rules for.
            key: The key to validate the rules with.
        """
        self._namespace = namespace
        self._key = key

    def is_valid(self, rule: Rule) -> bool:
        """Return True iff the rule is properly signed."""
        if isinstance(rule, ResultOfDataIn):
            namespace = rule.data_asset[3:].split('.')[0]
        elif isinstance(rule, ResultOfComputeIn):
            namespace = rule.compute_asset[3:].split('.')[0]
        elif isinstance(rule, MayAccess):
            namespace = rule.asset[3:].split('.')[0]
        elif isinstance(rule, InAssetCollection):
            namespace = rule.asset[3:].split('.')[0]
        elif isinstance(rule, InPartyCollection):
            namespace = rule.collection[3:].split('.')[0]

        if namespace != self._namespace:
            return False
        return rule.has_valid_signature(self._key)


class PolicySource(IPolicyCollection):
    """Ties together various sources of policies."""
    def __init__(
            self, registry_client: RegistryClient, site_validator: Validator
            ) -> None:
        """Create a PolicySource.

        This will automatically keep the replicas up-to-date as needed.

        Args:
            registry_client: A RegistryClient to use for getting
                servers.
            site_validator: A REST Validator for the Site API.
        """
        self._registry_client = registry_client
        self._site_validator = site_validator

        self._policy_replicas = dict()  # type: Dict[str, Replica[Rule]]
        self._registry_client.register_callback(self.on_update)

    def policies(self) -> Iterable[Rule]:
        """Returns the collected rules."""
        self._update()
        return [
                rule
                for replica in self._policy_replicas.values()
                for rule in replica.objects]

    def on_update(
            self, created: Set[RegisteredObject],
            deleted: Set[RegisteredObject]
            ) -> None:
        """Called when sites and/or parties appear or disappear.

        This is called by the RegistryClient whenever there's a change
        in the local registry replica. In response, we update our list
        of policy replicas to match the new and removed sites.

        Args:
            created: Set of new objects.
            deleted: Set of removed objects.
        """
        for o in created:
            if isinstance(o, SiteDescription) and o.namespace:
                client = PolicyClient(
                        o.endpoint + '/rules/updates', self._site_validator)

                key = self._registry_client.get_public_key_for_ns(o.namespace)
                validator = RuleValidator(o.namespace, key)
                self._policy_replicas[o.namespace] = Replica[Rule](
                        client, validator)

        for o in deleted:
            if isinstance(o, SiteDescription) and o.namespace:
                del(self._policy_replicas[o.namespace])

    def _update(self) -> None:
        """Ensures policy replicas are up to date."""
        self._registry_client.update()
        # The above calls back on_update(), which adds and removes
        # replicas as needed, so now we just need to update them.
        for replica in self._policy_replicas.values():
            replica.update()


class PolicyStore(CanonicalStore[Rule]):
    """A canonical store for policy rules."""
    UpdateType = PolicyUpdate
