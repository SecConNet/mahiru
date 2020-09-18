"""Central registry of remote-accessible things."""
from typing import Any, Dict, List, Optional, Set, Tuple, Type, TypeVar, Union

from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey

from proof_of_concept.definitions import (
        IAssetStore, ILocalWorkflowRunner, IPolicyServer)
from proof_of_concept.replication import (
        CanonicalStore, ReplicableArchive, ReplicationServer)


class PartyDescription:
    """Describes a Party to the rest of the DDM.

    Attributes:
        name: Name of the party.
        public_key: The party's public key for signing rules.

    """
    def __init__(self, name: str, public_key: RSAPublicKey) -> None:
        """Create a PartyDescription.

        Args:
            name: Name of the party.
            public_key: The party's public key for signing rules.
        """
        self.name = name
        self.public_key = public_key


class SiteDescription:
    """Describes a site to the rest of the DDM.

    Attributes:
        name: Name of the site.
        owner: Party which owns this site.
        admin: Party which administrates this site.
        runner: This site's local workflow runner.
        store: This site's asset store.
        namespace: The namespace managed by this site's policy server.
        policy_server: This site's policy server.

    """
    def __init__(
            self,
            name: str,
            owner: PartyDescription,
            admin: PartyDescription,
            runner: Optional[ILocalWorkflowRunner],
            store: Optional[IAssetStore],
            namespace: Optional[str],
            policy_server: Optional[IPolicyServer]
            ) -> None:
        """Create a SiteDescription.

        Args:
            name: Name of the site.
            owner: Party which owns this site.
            admin: Party which administrates this site.
            runner: This site's local workflow runner.
            store: This site's asset store.
            namespace: The namespace managed by this site's policy
                server.
            policy_server: This site's policy server.

        """
        self.name = name
        self.owner = owner
        self.admin = admin
        self.runner = runner
        self.store = store
        self.namespace = namespace
        self.policy_server = policy_server

        if store is None and runner is not None:
            raise RuntimeError('Site with runner needs a store')

        if namespace is None and policy_server is not None:
            raise RuntimeError('Policy server specified without namespace')

        if namespace is not None and policy_server is None:
            raise RuntimeError('Namespace specified but policy server missing')


RegisteredObject = Union[PartyDescription, SiteDescription]


_ReplicatedClass = TypeVar('_ReplicatedClass', bound=RegisteredObject)


class Registry:
    """Global registry of remote-accessible things.

    Registers runners, stores, and assets. In a real system, runners
    and stores would be identified by a URL, and use the DNS to
    resolve. For now the registry helps with this.
    """
    def __init__(self) -> None:
        """Create a new registry."""
        self._asset_locations = dict()           # type: Dict[str, str]

        archive = ReplicableArchive[RegisteredObject]()
        self._store = CanonicalStore[RegisteredObject](archive)
        self.replication_server = ReplicationServer[RegisteredObject](
                archive, 1.0)

    def register_party(
            self, name: str, public_key: RSAPublicKey) -> None:
        """Register a party with the DDM.

        Args:
            name: Name of the party.
            public_key: Public key of this party.
        """
        if self._in_store(PartyDescription, 'name', name):
            raise RuntimeError(f'There is already a party called {name}')

        party_desc = PartyDescription(name, public_key)
        self._store.insert(party_desc)

    def register_site(
            self,
            name: str,
            owner_name: str,
            admin_name: str,
            runner: Optional[ILocalWorkflowRunner],
            store: Optional[IAssetStore],
            namespace: Optional[str],
            policy_server: Optional[IPolicyServer]
            ) -> None:
        """Register a Site with the Registry.

        Args:
            name: Name of the site.
            owner_name: Party owning this site.
            admin_name: Party administrating this site.
            runner: This site's local workflow runner.
            store: This site's asset store.
            namespace: The namespace managed by this site's policy
                server.
            policy_server: This site's policy server.

        """
        if self._in_store(SiteDescription, 'name', name):
            raise RuntimeError(f'There is already a site called {name}')

        owner = self._get_object(PartyDescription, 'name', owner_name)
        if owner is None:
            raise RuntimeError(f'Party {owner_name} not found')

        admin = self._get_object(PartyDescription, 'name', admin_name)
        if admin is None:
            raise RuntimeError(f'Party {admin_name} not found')

        site_desc = SiteDescription(
                name, owner, admin, runner, store, namespace, policy_server)
        self._store.insert(site_desc)

    def register_asset(self, asset_id: str, site_name: str) -> None:
        """Register an Asset with the Registry.

        Args:
            asset_id: The id of the asset to register.
            site_name: Name of the site where it can be found.
        """
        if asset_id in self._asset_locations:
            raise RuntimeError('There is already an asset with this name')
        self._asset_locations[asset_id] = site_name

    def get_asset_location(self, asset_id: str) -> str:
        """Returns the name of the site this asset is in.

        Args:
            asset_id: ID of the asset to find.

        Return:
            The site it can be found at.

        Raises:
            KeyError: If no asset with the given id is registered.
        """
        return self._asset_locations[asset_id]

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


global_registry = Registry()
