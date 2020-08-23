"""Central registry of remote-accessible things."""
from typing import Dict, List, Set, Tuple

from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey

from proof_of_concept.definitions import (
        IAssetStore, ILocalWorkflowRunner, IPolicyServer)
from proof_of_concept.replication import (
        CanonicalStore, ReplicableArchive, ReplicationServer)


class RegisteredObject:
    """A parent class for DDM-wide metadata classes.

    This is here mainly because it's required for the replication
    system.

    """
    pass


class PartyDescription(RegisteredObject):
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


class NamespaceDescription(RegisteredObject):
    """Describes a namespace to the rest of the DDM.

    Attributes:
        name: Name of the namespace (i.e. the prefix)
        owner: Party which owns the namespace and assets in it.

    """
    def __init__(self, name: str, owner: PartyDescription) -> None:
        """Create a NamespaceDescription.

        Args:
            name: Name of the namespace (i.e. the prefix)
            owner: Party which owns the namespace and assets in it.

        """
        self.name = name
        self.owner = owner


class SiteDescription(RegisteredObject):
    """Describes a site to the rest of the DDM.

    Attributes:
        name: Name of the site.
        admin: Party which administrates this site.

    """
    def __init__(self, name: str, admin: PartyDescription) -> None:
        """Create a SiteDescription.

        Args:
            name: Name of the site.
            admin: Party which administrates this site.

        """
        self.name = name
        self.admin = admin


class RunnerDescription(RegisteredObject):
    """Describes a workflow runner to the rest of the DDM.

    Attributes:
        site: Site at which this runner is located.
        runner: The runner service.

    """
    def __init__(
            self, site: SiteDescription, runner: ILocalWorkflowRunner
            ) -> None:
        """Create a RunnerDescription.

        Args:
            site: Site at which this runner is located.
            runner: The runner service.

        """
        self.site = site
        self.runner = runner


class AssetStoreDescription(RegisteredObject):
    """Describes an asset store to the rest of the DDM.

    Attributes:
        site: The site at which this store is located.
        store: The store service.

    """
    def __init__(
            self, site: SiteDescription, store: IAssetStore
            ) -> None:
        """Create an AssetStoreDescription.

        Args:
            site: The site at which this store is located.
            store: The store service.

        """
        self.site = site
        self.store = store


class PolicyServerDescription(RegisteredObject):
    """Describes a policy server to the rest of the DDM.

    Attributes:
        namespace: The namespace governed by this server.
        site: The site at which this server is located.

    """
    def __init__(
            self, site: SiteDescription, namespace: NamespaceDescription,
            server: IPolicyServer) -> None:
        """Create a PolicyServerDescription.

        Args:
            site: The site at which this server is located.
            namespace: The namespace governed by this server.
            server: The policy server.

        """
        self.site = site
        self.namespace = namespace
        self.server = server


class Registry:
    """Global registry of remote-accessible things.

    Registers runners, stores, and assets. In a real system, runners
    and stores would be identified by a URL, and use the DNS to
    resolve. For now the registry helps with this.
    """
    def __init__(self) -> None:
        """Create a new registry."""
        self._assets = dict()           # type: Dict[str, str]

        self._archive = ReplicableArchive[RegisteredObject]()
        self._store = CanonicalStore[RegisteredObject](self._archive)
        self.replication_server = ReplicationServer[RegisteredObject](
                self._archive, 1.0)

    def register_party(
            self, name: str, namespace: str, public_key: RSAPublicKey) -> None:
        """Register a party with the DDM.

        Args:
            name: Name of the party.
            namespace: ID namespace owned by this party.
            public_key: Public key of this party.
        """
        for i in self._store.objects():
            if isinstance(i, PartyDescription) and i.name == name:
                raise RuntimeError('There is already a party called {}'.format(
                    name))

        party_desc = PartyDescription(name, public_key)
        self._store.insert(party_desc)
        self._store.insert(NamespaceDescription(namespace, party_desc))

    def register_site(self, name: str, admin_name: str) -> None:
        """Register a Site with the Registry.

        Args:
            name: Name of the site.
            admin_name: Party administrating this site.

        """
        for o in self._store.objects():
            if isinstance(o, SiteDescription) and o.name == name:
                raise RuntimeError('There is already a site called {}'.format(
                    name))

        for o in self._store.objects():
            if isinstance(o, PartyDescription) and o.name == admin_name:
                admin = o
                break
        else:
            raise RuntimeError('Party {} not found'.format(admin_name))

        site_desc = SiteDescription(name, admin)
        self._store.insert(site_desc)

    def register_runner(
            self, site_name: str, admin: str, runner: ILocalWorkflowRunner
            ) -> None:
        """Register a LocalWorkflowRunner with the Registry.

        Args:
            site_name: Name of the site where the runner is located.
            admin: The party administrating this runner.
            runner: The runner to register.
        """
        for i in self._store.objects():
            if isinstance(i, RunnerDescription) and i.runner == runner:
                raise RuntimeError(
                        'There is already a runner called {}'.format(
                            runner.name))

        site = self._get_site(site_name)
        runner_desc = RunnerDescription(site, runner)
        self._store.insert(runner_desc)

    def register_store(self, site_name: str, store: IAssetStore) -> None:
        """Register an AssetStore with the Registry.

        Args:
            site_name: The site this store is located at.
            store: The data store to register.
        """
        for o in self._store.objects():
            if isinstance(o, AssetStoreDescription) and o.store == store:
                raise RuntimeError(
                        'There is already a store called {}'.format(
                            store.name))

        site = self._get_site(site_name)
        store_desc = AssetStoreDescription(site, store)
        self._store.insert(store_desc)

    def register_policy_server(
            self, site_name: str, namespace_name: str, server: IPolicyServer
            ) -> None:
        """Register a PolicyServer with the registry.

        Args:
            site_name: Site at which this server is located.
            namespace_name: The namespace this server serves policies
                    for.
            server: The data store to register.
        """
        for o in self._store.objects():
            if isinstance(o, PolicyServerDescription) and o.server == server:
                raise RuntimeError('The server is already registered')

        site = self._get_site(site_name)

        for o in self._store.objects():
            if isinstance(o, NamespaceDescription):
                if o.name == namespace_name:
                    namespace = o
                    break
        else:
            raise RuntimeError('Namespace {} not found'.format(namespace_name))

        server_desc = PolicyServerDescription(site, namespace, server)
        self._store.insert(server_desc)

    def register_asset(self, asset_id: str, store_name: str) -> None:
        """Register an Asset with the Registry.

        Args:
            asset_id: The id of the asset to register.
            store_name: Name of the store where it can be found.
        """
        if asset_id in self._assets:
            raise RuntimeError('There is already an asset with this name')
        self._assets[asset_id] = store_name

    def get_asset_location(self, asset_id: str) -> str:
        """Returns the name of the store this asset is in.

        Args:
            asset_id: ID of the asset to find.

        Return:
            The store it can be found in.

        Raises:
            KeyError: If no asset with the given id is registered.
        """
        return self._assets[asset_id]

    def _get_site(self, site_name: str) -> SiteDescription:
        """Returns the site of the given name, if registered."""
        for o in self._store.objects():
            if isinstance(o, SiteDescription) and o.name == site_name:
                return o
        else:
            raise RuntimeError('Site {} not found'.format(site_name))


global_registry = Registry()
