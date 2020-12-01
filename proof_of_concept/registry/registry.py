"""Central registry of remote-accessible things."""
import logging
from typing import Any, Dict, Optional, Type, TypeVar

from proof_of_concept.definitions.identifier import Identifier
from proof_of_concept.definitions.interfaces import IAssetStore
from proof_of_concept.definitions.registry import (
        PartyDescription, RegisteredObject, SiteDescription)
from proof_of_concept.registry.replication import RegistryStore, RegistryUpdate
from proof_of_concept.replication import ReplicableArchive


logger = logging.getLogger(__name__)


_ReplicatedClass = TypeVar('_ReplicatedClass', bound=RegisteredObject)


class Registry:
    """Global registry of remote-accessible things.

    Registers runners, stores, and assets. In a real system, runners
    and stores would be identified by a URL, and use the DNS to
    resolve. For now the registry helps with this.
    """
    def __init__(self) -> None:
        """Create a new registry."""
        self._asset_locations = dict()           # type: Dict[Identifier, str]

        archive = ReplicableArchive[RegisteredObject]()
        self.store = RegistryStore(archive, 0.1)

    def register_party(
            self, description: PartyDescription) -> None:
        """Register a party with the DDM.

        Args:
            description: A description of the party
        """
        if self._in_store(PartyDescription, 'name', description.name):
            raise RuntimeError(
                    f'There is already a party called {description.name}')

        self.store.insert(description)
        logger.info(f'Registered party {description}')

    def deregister_party(self, name: str) -> None:
        """Deregister a party with the DDM.

        Args:
            name: Name of the party to deregister.
        """
        description = self._get_object(PartyDescription, 'name', name)
        if description is None:
            raise KeyError('Party not found')
        self.store.delete(description)

    def register_site(self, description: SiteDescription) -> None:
        """Register a Site with the Registry.

        Args:
            description: Description of the site.

        """
        if self._in_store(SiteDescription, 'name', description.name):
            raise RuntimeError(
                    f'There is already a site called {description.name}')

        owner = self._get_object(
                PartyDescription, 'name', description.owner_name)
        if owner is None:
            raise RuntimeError(f'Party {description.owner_name} not found')

        admin = self._get_object(
                PartyDescription, 'name', description.admin_name)
        if admin is None:
            raise RuntimeError(f'Party {description.admin_name} not found')

        self.store.insert(description)
        logger.info(f'{self} Registered site {description}')

    def deregister_site(self, name: str) -> None:
        """Deregister a site with the DDM.

        Args:
            name: Name of the site to deregister.
        """
        description = self._get_object(SiteDescription, 'name', name)
        if description is None:
            raise KeyError('Site not found')
        self.store.delete(description)

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
        for o in self.store.objects():
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
