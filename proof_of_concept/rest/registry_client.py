"""Client for the registry REST API."""
from pathlib import Path
from typing import cast

import requests
import ruamel.yaml as yaml

from proof_of_concept.definitions.identifier import Identifier
from proof_of_concept.definitions.interfaces import (
        IRegistration, IRegistryService, IReplicaUpdate)
from proof_of_concept.definitions.registry import (
        PartyDescription, RegisteredObject, SiteDescription)
from proof_of_concept.rest.replication import ReplicationRestClient
from proof_of_concept.registry.replication import RegistryUpdate
from proof_of_concept.rest.serialization import serialize


class _ReplicationRestClient(ReplicationRestClient[RegisteredObject]):
    """A replication client for replicating the registry."""
    UpdateType = RegistryUpdate


class RegistryRestClient(IRegistryService):
    """REST client for the global registry.

    This connects to the read-only replication part of the registry
    REST API.

    """
    def __init__(self, endpoint: str = 'http://localhost:4413') -> None:
        """Create a RegistryRestClient.

        Args:
            endpoint: URL of the endpoint to connect to.

        """
        registry_api_file = (
                Path(__file__).parents[1] / 'rest' / 'registry_api.yaml')

        with open(registry_api_file, 'r') as f:
            registry_api_def = yaml.safe_load(f.read())

        self._registry_client = _ReplicationRestClient(endpoint + '/updates')

    def get_updates_since(self, from_version: int) -> RegistryUpdate:
        """Return a set of objects modified since the given version.

        Args:
            from_version: A version received from a previous call to
                    this function, or 0 to get an update for a
                    fresh replica.

        Return:
            An update from the given version to a newer version.
        """
        # This is always a RegistryUpdate, because we set UpdateType in
        # _ReplicationRestClient above. But mypy doesn't quite let us
        # write the type annotations to tell it that, so we cast.
        return cast(
                RegistryUpdate,
                self._registry_client.get_updates_since(from_version))


class RegistrationRestClient(IRegistration):
    """REST client for registering sites and parties.

    This connects to the registration part of the registry REST API.

    """
    def __init__(self, endpoint: str = 'http://localhost:4413') -> None:
        """Create a RegistrationRestClient."""
        self._registry_endpoint = endpoint

    def register_party(self, description: PartyDescription) -> None:
        """Register a party with the Registry.

        Args:
            description: Description of the party.

        """
        requests.post(
                self._registry_endpoint + '/parties',
                json=serialize(description))

    def deregister_party(self, party: Identifier) -> None:
        """Deregister a party with the Registry.

        Args:
            party: The party to deregister.

        """
        r = requests.delete(f'{self._registry_endpoint}/parties/{party}')
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

    def deregister_site(self, site: Identifier) -> None:
        """Deregister a site with the Registry.

        Args:
            site: The site to deregister.

        """
        r = requests.delete(f'{self._registry_endpoint}/sites/{site}')
        if r.status_code == 404:
            raise KeyError('Site not found')
