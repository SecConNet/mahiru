"""Functionality for connecting to the central registry."""
from pathlib import Path
from typing import Any, Callable, List, Optional, Set

from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey

from proof_of_concept.definitions.identifier import Identifier
from proof_of_concept.definitions.interfaces import IRegistry
from proof_of_concept.definitions.registry import (
        PartyDescription, RegisteredObject, SiteDescription)
from proof_of_concept.replication import Replica


RegistryCallback = Callable[
        [Set[RegisteredObject], Set[RegisteredObject]], None]


class _RegistryReplica(Replica[RegisteredObject]):
    """Local replica of the global registry."""
    pass


class RegistryClient:
    """Local interface to the global registry."""
    def __init__(self, registry: IRegistry) -> None:
        """Create a RegistryClient.

        Note that this class can use either a Registry object to
        call directly, or a RegistryRestClient to connect to a
        registry via a REST API.

        Args:
            registry: The registry to connect to.
        """
        self._registry = registry
        self._callbacks = list()    # type: List[RegistryCallback]
        self._registry_replica = _RegistryReplica(
                registry, on_update=self._on_registry_update)

        # Get initial data
        self._registry_replica.update()

    def register_callback(self, callback: RegistryCallback) -> None:
        """Register a callback for registry updates.

        The callback will be called immediately with a set of all
        current records as the first argument. After that, it will
        be called with newly created records as the first argument
        and newly deleted records as the second argument whenever
        the registry replica is updated.

        Args:
            callback: The function to call.

        """
        self._callbacks.append(callback)
        callback(self._registry_replica.objects, set())

    def update(self) -> None:
        """Ensures the local registry information is up-to-date.

        If the registry is updated, this will call any registered
        callback functions with the changes.

        """
        self._registry_replica.update()

    def register_party(self, description: PartyDescription) -> None:
        """Register a party with the Registry.

        Args:
            description: Description of the party.

        """
        self._registry.register_party(description)

    def deregister_party(self, party: Identifier) -> None:
        """Deregister a party with the Registry.

        Args:
            party: The party to deregister.

        Raises:
            KeyError: If the party could not be found

        """
        self._registry.deregister_party(party)

    def register_site(self, description: SiteDescription) -> None:
        """Register a site with the Registry.

        Args:
            description: Description of the site.

        """
        self._registry.register_site(description)

    def deregister_site(self, site: Identifier) -> None:
        """Deregister a site with the Registry.

        Args:
            site: The site to deregister.

        Raises:
            KeyError: If the site could not be found

        """
        self._registry.deregister_site(site)

    def get_public_key_for_ns(self, namespace: str) -> RSAPublicKey:
        """Get the public key of the owner of a namespace."""
        # Do not update here, when this is called we're processing one
        # already.
        site = self._get_site('namespace', namespace)
        if site is not None:
            owner = self._get_party(site.owner_id)
            if owner is None:
                raise RuntimeError(f'Registry replica is broken')
            return owner.public_key
        raise RuntimeError(f'No site with namespace {namespace} found')

    def list_sites_with_runners(self) -> List[str]:
        """Returns a list of id's of sites with runners."""
        self.update()
        sites = list()    # type: List[str]
        for o in self._registry_replica.objects:
            if isinstance(o, SiteDescription):
                if o.runner:
                    sites.append(o.id)
        return sites

    def get_site_by_id(self, site_id: Identifier) -> SiteDescription:
        """Gets a site's description by id.

        Args:
            site_id: Identifier of the site to look up.

        Returns:
            The description of the corresponding site.

        Raises:
            KeyError: If no site with that id exists.

        """
        site = self._get_site('id', site_id)
        if not site:
            raise KeyError(f'Site with id {site_id} not found')
        return site

    def _get_party(self, party_id: Identifier) -> Optional[PartyDescription]:
        """Returns the party with the given id."""
        for o in self._registry_replica.objects:
            if isinstance(o, PartyDescription):
                if o.id == party_id:
                    return o
        return None

    def _get_site(
            self, attr_name: str, value: Any) -> Optional[SiteDescription]:
        """Returns a site with a given attribute value."""
        for o in self._registry_replica.objects:
            if isinstance(o, SiteDescription):
                a = getattr(o, attr_name)
                if a is not None and a == value:
                    return o
        return None

    def _on_registry_update(
            self, created: Set[RegisteredObject],
            deleted: Set[RegisteredObject]) -> None:
        """Calls callbacks when sites are updated."""
        for callback in self._callbacks:
            callback(created, deleted)
