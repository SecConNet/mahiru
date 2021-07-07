"""Central registry of remote-accessible things."""
import logging
from typing import Any, Dict, Optional, Type, TypeVar

from proof_of_concept.definitions.identifier import Identifier
from proof_of_concept.definitions.interfaces import (
        IAssetStore, IRegistration, IRegistryService, IReplicaUpdate)
from proof_of_concept.definitions.registry import (
        PartyDescription, RegisteredObject, SiteDescription)
from proof_of_concept.registry.replication import RegistryStore
from proof_of_concept.replication import ReplicableArchive


logger = logging.getLogger(__name__)


_ReplicatedClass = TypeVar('_ReplicatedClass', bound=RegisteredObject)


class Registry(IRegistration, IRegistryService):
    """Global registry of remote-accessible things.

    Registers runners, stores, and assets. In a real system, runners
    and stores would be identified by a URL, and use the DNS to
    resolve. For now the registry helps with this.
    """
    def __init__(self) -> None:
        """Create a new registry."""
        self._asset_locations = dict()           # type: Dict[Identifier, str]

        archive = ReplicableArchive[RegisteredObject]()
        self._store = RegistryStore(archive, 0.1)

    def get_updates_since(
            self, from_version: int) -> IReplicaUpdate[RegisteredObject]:
        """Return a set of objects modified since the given version.

        Args:
            from_version: A version received from a previous call to
                    this function, or 0 to get an update for a
                    fresh replica.

        Return:
            An update from the given version to a newer version.
        """
        return self._store.get_updates_since(from_version)

    def register_party(
            self, description: PartyDescription) -> None:
        """Register a party with the DDM.

        Args:
            description: A description of the party
        """
        if self._in_store(PartyDescription, 'id', description.id):
            raise RuntimeError(
                    f'There is already a party called {description.id}')

        self._store.insert(description)
        logger.info(f'Registered party {description}')

    def deregister_party(self, party_id: Identifier) -> None:
        """Deregister a party with the DDM.

        Args:
            party_id: Identifier of the party to deregister.
        """
        description = self._get_object(PartyDescription, 'id', party_id)
        if description is None:
            raise KeyError('Party not found')
        self._store.delete(description)

    def register_site(self, description: SiteDescription) -> None:
        """Register a Site with the Registry.

        Args:
            description: Description of the site.

        """
        if self._in_store(SiteDescription, 'id', description.id):
            raise RuntimeError(
                    f'There is already a site called {description.id}')

        owner = self._get_object(
                PartyDescription, 'id', description.owner_id)
        if owner is None:
            raise RuntimeError(f'Party {description.owner_id} not found')

        admin = self._get_object(
                PartyDescription, 'id', description.admin_id)
        if admin is None:
            raise RuntimeError(f'Party {description.admin_id} not found')

        self._store.insert(description)
        logger.info(f'{self} Registered site {description}')

    def deregister_site(self, site_id: Identifier) -> None:
        """Deregister a site with the DDM.

        Args:
            site_id: Identifer of the site to deregister.
        """
        description = self._get_object(SiteDescription, 'id', site_id)
        if description is None:
            raise KeyError('Site not found')
        self._store.delete(description)

    def _get_object(
            self, typ: Type[_ReplicatedClass], attr_name: str, value: Any
            ) -> Optional[_ReplicatedClass]:
        """Returns an object from the store.

        Searches the store for an object of type `typ` that has value
        `value` for its attribute named `attr_name`. If there are
        multiple such objects, one is returned at random.

        Args:
            typ: Type of object to consider, subclass of
                RegisteredObject.
            attr_name: Name of the attribute on that object to check.
            value: Value that the attribute must have.

        Returns:
            The object, if found, or None if no object was found.

        Raises:
            AttributeError if an object of type `typ` is encountered
                in the store which does not have an attribute named
                `attr_name`.
        """
        for o in self._store.objects():
            if isinstance(o, typ):
                if getattr(o, attr_name) == value:
                    return o
        return None

    def _in_store(
            self, typ: Type[_ReplicatedClass], attr_name: str, value: Any
            ) -> bool:
        """Returns True iff a matching object is in the store.

        Searches the store for an object of type `typ` that has value
        `value` for its attribute named `attr_name`, and returns True
        if there is at least one of those in the store.

        Args:
            typ: Type of object to consider, subclass of
                RegisteredObject.
            attr_name: Name of the attribute on that object to check.
            value: Value that the attribute must have.

        Returns:
            True if a matching object was found, False otherwise.

        Raises:
            AttributeError if an object of type `typ` is encountered
                in the store which does not have an attribute named
                `attr_name`.
        """
        return self._get_object(typ, attr_name, value) is not None
