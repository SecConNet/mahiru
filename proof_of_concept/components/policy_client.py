"""Component for making DDM policies available locally."""
from typing import Dict, Iterable, Set

from proof_of_concept.components.registry_client import RegistryClient
from proof_of_concept.definitions.interfaces import IPolicyCollection
from proof_of_concept.definitions.registry import (
        RegisteredObject, SiteDescription)
from proof_of_concept.definitions.policy import Rule
from proof_of_concept.policy.replication import RuleValidator
from proof_of_concept.replication import Replica
from proof_of_concept.rest.replication import PolicyRestClient
from proof_of_concept.rest.validation import site_validator


class PolicyClient(IPolicyCollection):
    """Ties together various sources of policies."""
    def __init__(self, registry_client: RegistryClient) -> None:
        """Create a PolicyClient.

        This will automatically keep the replicas up-to-date as needed.

        Args:
            registry_client: A RegistryClient to use for getting
                servers.
        """
        self._registry_client = registry_client

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
                client = PolicyRestClient(
                        o.endpoint + '/rules/updates', site_validator)

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
