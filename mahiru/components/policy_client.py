"""Component for making DDM policies available locally."""
from pathlib import Path
from typing import Dict, Iterable, Optional, Set, Tuple

from mahiru.components.registry_client import RegistryClient
from mahiru.definitions.identifier import Identifier
from mahiru.definitions.interfaces import IPolicyCollection
from mahiru.definitions.registry import RegisteredObject, SiteDescription
from mahiru.definitions.policy import Rule
from mahiru.policy.replication import RuleValidator
from mahiru.replication import Replica
from mahiru.rest.replication import PolicyRestClient


class PolicyClient(IPolicyCollection):
    """Ties together various sources of policies."""
    def __init__(
            self, registry_client: RegistryClient,
            trust_store: Optional[Path] = None,
            client_credentials: Optional[Tuple[Path, Path]] = None
            ) -> None:
        """Create a PolicyClient.

        This will automatically keep the replicas up-to-date as needed.

        Args:
            registry_client: A RegistryClient to use for getting
                servers.
            trust_store: A file with trusted certificates/anchors.
            client_credentials: Paths to PEM files with the HTTPS
                    client certificate and key to use when connecting.
        """
        self._registry_client = registry_client
        self._trust_store = trust_store
        self._client_credentials = client_credentials

        self._policy_replicas = dict()  # type: Dict[Identifier, Replica[Rule]]
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
        for o in deleted:
            if isinstance(o, SiteDescription) and o.has_policies:
                del(self._policy_replicas[o.id])

        for o in created:
            if isinstance(o, SiteDescription) and o.has_policies:
                client = PolicyRestClient(
                        o.endpoint + '/rules/updates', self._trust_store,
                        self._client_credentials)

                namespace, key = self._registry_client.get_ns_and_key(
                        o.owner_id)
                validator = RuleValidator(namespace, key)
                self._policy_replicas[o.id] = Replica[Rule](
                        client, validator)

    def _update(self) -> None:
        """Ensures policy replicas are up to date."""
        self._registry_client.update()
        # The above calls back on_update(), which adds and removes
        # replicas as needed, so now we just need to update them.
        for replica in self._policy_replicas.values():
            replica.update()
