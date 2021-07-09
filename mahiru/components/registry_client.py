"""Functionality for connecting to the central registry."""
from pathlib import Path
from typing import Any, Callable, List, Optional, Set

from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey

from mahiru.definitions.identifier import Identifier
from mahiru.definitions.interfaces import IRegistryService
from mahiru.definitions.registry import (
        PartyDescription, RegisteredObject, SiteDescription)
from mahiru.replication import Replica


RegistryCallback = Callable[
        [Set[RegisteredObject], Set[RegisteredObject]], None]


class _RegistryReplica(Replica[RegisteredObject]):
    """Local replica of the global registry."""
    pass


class RegistryClient:
    """Local client for the global registry.

    This provides read-only access to the global registry via several
    utility functions, based on a local replica it keeps.

    """
    def __init__(self, registry: IRegistryService) -> None:
        """Create a RegistryClient.

        Note that this class can use either a Registry object to
        call directly, or a RegistryRestClient to connect to a
        registry via a REST API.

        Args:
            registry: The registry to connect to.
        """
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

    def list_sites_with_runners(self) -> List[Identifier]:
        """Returns a list of id's of sites with runners."""
        self.update()
        sites = list()
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
