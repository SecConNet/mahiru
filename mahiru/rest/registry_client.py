"""Client for the registry REST API."""
from pathlib import Path
from typing import cast, Optional, Union

import requests
import ruamel.yaml as yaml

from mahiru.definitions.identifier import Identifier
from mahiru.definitions.interfaces import (
        IRegistration, IRegistryService, IReplicaUpdate)
from mahiru.definitions.registry import (
        PartyDescription, RegisteredObject, SiteDescription)
from mahiru.rest.replication import ReplicationRestClient
from mahiru.registry.replication import RegistryUpdate
from mahiru.rest.serialization import serialize


class RegistryRestClient(ReplicationRestClient[RegisteredObject]):
    """A replication client for replicating the registry."""
    UpdateType = RegistryUpdate

    def __init__(
            self, endpoint: str = 'http://localhost:4413',
            trust_store: Optional[Path] = None) -> None:
        """Create a RegistryRestClient.

        Args:
            endpoint: URL of the endpoint to connect to.
            trust_store: A file with trusted certificates/anchors.
        """
        super().__init__(endpoint + '/updates', trust_store)


class RegistrationRestClient(IRegistration):
    """REST client for registering sites and parties.

    This connects to the registration part of the registry REST API.

    """
    def __init__(
            self, endpoint: str = 'http://localhost:4413',
            trust_store: Optional[Path] = None) -> None:
        """Create a RegistrationRestClient."""
        self._registry_endpoint = endpoint
        if trust_store:
            self._verify = str(trust_store)     # type: Union[str, bool]
        else:
            self._verify = True

    def register_party(self, description: PartyDescription) -> None:
        """Register a party with the Registry.

        Args:
            description: Description of the party.

        """
        requests.post(
                self._registry_endpoint + '/parties',
                json=serialize(description), verify=self._verify)

    def deregister_party(self, party: Identifier) -> None:
        """Deregister a party with the Registry.

        Args:
            party: The party to deregister.

        """
        r = requests.delete(
                f'{self._registry_endpoint}/parties/{party}',
                verify=self._verify)

        if r.status_code == 404:
            raise KeyError('Party not found')

    def register_site(self, description: SiteDescription) -> None:
        """Register a site with the Registry.

        Args:
            description: Description of the site.

        """
        requests.post(
                self._registry_endpoint + '/sites',
                json=serialize(description), verify=self._verify)

    def deregister_site(self, site: Identifier) -> None:
        """Deregister a site with the Registry.

        Args:
            site: The site to deregister.

        """
        r = requests.delete(
                f'{self._registry_endpoint}/sites/{site}',
                verify=self._verify)
        if r.status_code == 404:
            raise KeyError('Site not found')
