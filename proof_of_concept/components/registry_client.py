"""Functionality for connecting to the central registry."""
from pathlib import Path
import requests
from typing import Any, Callable, List, Optional, Set

from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey
import ruamel.yaml as yaml

from proof_of_concept.definitions.assets import AssetId
from proof_of_concept.definitions.registry import (
        PartyDescription, RegisteredObject, SiteDescription)
from proof_of_concept.rest.serialization import serialize
from proof_of_concept.registry.registry import global_registry
from proof_of_concept.replication import Replica
from proof_of_concept.rest.replication import RegistryRestClient
from proof_of_concept.rest.validation import Validator


RegistryCallback = Callable[
        [Set[RegisteredObject], Set[RegisteredObject]], None]


class RegistryReplica(Replica[RegisteredObject]):
    """Local replica of the global registry."""
    pass


class RegistryClient:
    """Local interface to the global registry."""
    def __init__(self) -> None:
        """Create a RegistryClient."""
        # TODO: This will be passed in as an argument later.
        self._registry_endpoint = 'http://localhost:4413'

        self._callbacks = list()    # type: List[RegistryCallback]

        # Set up connection to registry
        registry_api_file = (
                Path(__file__).parents[1] / 'rest' / 'registry_api.yaml')
        with open(registry_api_file, 'r') as f:
            registry_api_def = yaml.safe_load(f.read())

        registry_validator = Validator(registry_api_def)

        registry_client = RegistryRestClient(
                self._registry_endpoint + '/updates', registry_validator)

        self._registry_replica = RegistryReplica(
                registry_client, on_update=self._on_registry_update)

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
        requests.post(
                self._registry_endpoint + '/parties',
                json=serialize(description))

    def deregister_party(self, name: str) -> None:
        """Deregister a party with the Registry.

        Args:
            name: Name of the party to deregister.

        """
        r = requests.delete(f'{self._registry_endpoint}/parties/{name}')
        if r.status_code == 404:
            raise KeyError('Party not found')

    def register_site(self, description: SiteDescription) -> None:
        """Register a site with the Registry.

        Args:
            description: Description of the site.

        """
        requests.post(
                self._registry_endpoint + '/sites',
                json=serialize(description))

    def deregister_site(self, name: str) -> None:
        """Deregister a site with the Registry.

        Args:
            name: Name of the site to deregister.

        """
        r = requests.delete(f'{self._registry_endpoint}/sites/{name}')
        if r.status_code == 404:
            raise KeyError('Site not found')

    def register_asset(self, asset_id: AssetId, site_name: str) -> None:
        """Register an Asset with the Registry.

        Args:
            asset_id: The id of the asset to register.
            site_name: Name of the site where it can be found.

        """
        global_registry.register_asset(asset_id, site_name)

    def get_public_key_for_ns(self, namespace: str) -> RSAPublicKey:
        """Get the public key of the owner of a namespace."""
        # Do not update here, when this is called we're processing one
        # already.
        site = self._get_site('namespace', namespace)
        if site is not None:
            owner = self._get_party(site.owner_name)
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
                    sites.append(o.name)
        return sites

    def get_site_by_name(self, site_name: str) -> SiteDescription:
        """Gets a site's description by name.

        Args:
            site_name: Name of the site to look up.

        Returns:
            The description of the corresponding site.

        Raises:
            KeyError: If no site with that name exists.

        """
        site = self._get_site('name', site_name)
        if not site:
            raise KeyError(f'Site named {site_name} not found')
        return site

    @staticmethod
    def get_asset_location(asset_id: AssetId) -> str:
        """Returns the name of the site which stores this asset."""
        return global_registry.get_asset_location(asset_id)

    def _get_party(self, name: str) -> Optional[PartyDescription]:
        """Returns the party with the given name."""
        for o in self._registry_replica.objects:
            if isinstance(o, PartyDescription):
                if o.name == name:
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