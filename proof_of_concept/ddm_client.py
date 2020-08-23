"""Functionality for connecting to other DDM sites."""
from typing import Any, Dict, List, Optional, Tuple

from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey

from proof_of_concept.definitions import (
        IAssetStore, ILocalWorkflowRunner, IPolicyServer, Metadata, Plan)
from proof_of_concept.workflow import Job, Workflow
from proof_of_concept.registry import (
        global_registry, AssetStoreDescription, RegisteredObject,
        NamespaceDescription, PolicyServerDescription, RunnerDescription)
from proof_of_concept.replication import Replica


class DDMClient:
    """Handles connecting to global registry, runners and stores."""
    def __init__(self, party: str) -> None:
        """Create a DDMClient.

        Args:
            party: The party on whose behalf this client acts.
        """
        self._party = party
        self._registry_replica = Replica[RegisteredObject](
                global_registry.replication_server)

    def register_party(
            self, name: str, namespace: str, public_key: RSAPublicKey) -> None:
        """Register a party with the Registry.

        Args:
            name: Name of the party.
            namespace: ID namespace owned by this party.
            public_key: Public key of this party.
        """
        global_registry.register_party(name, namespace, public_key)

    def register_site(self, name: str, admin_name: str) -> None:
        """Register a site with the Registry.

        Args:
            name: Name of the site.
            admin_name: Name of the administrating party.
        """
        global_registry.register_site(name, admin_name)

    def register_runner(
            self, site_name: str, admin_name: str, runner: ILocalWorkflowRunner
            ) -> None:
        """Register a LocalWorkflowRunner with the Registry.

        Args:
            site_name: Name of the site where this runner is.
            admin_name: The party administrating this runner.
            runner: The runner to register.
        """
        global_registry.register_runner(site_name, admin_name, runner)

    def register_store(self, admin: str, store: IAssetStore) -> None:
        """Register a AssetStore with the Registry.

        Args:
            admin: The party administrating this runner.
            store: The data store to register.
        """
        global_registry.register_store(admin, store)

    def register_policy_server(
            self, site_name: str, namespace: str, server: IPolicyServer
            ) -> None:
        """Register a policy server with the registry.

        Args:
            site_name: The site at which this server is located.
            namespace: The namespace containing the assets this policy
                    server serves policy for.
            server: The data store to register.
        """
        global_registry.register_policy_server(site_name, namespace, server)

    def register_asset(self, asset_id: str, store_name: str) -> None:
        """Register an Asset with the Registry.

        Args:
            asset_id: The id of the asset to register.
            store_name: Name of the store where it can be found.
        """
        global_registry.register_asset(asset_id, store_name)

    def get_public_key_for_ns(self, namespace: str) -> RSAPublicKey:
        """Get the public key of the owner of a namespace."""
        owner = None
        self._registry_replica.update()
        for o in self._registry_replica.objects:
            if isinstance(o, NamespaceDescription) and o.name == namespace:
                return o.owner.public_key
        raise RuntimeError('Namespace {} not found'.format(namespace))

    def list_runners(self) -> List[str]:
        """Returns a list of id's of available runners."""
        self._registry_replica.update()
        runners = list()    # type: List[str]
        for o in self._registry_replica.objects:
            if isinstance(o, RunnerDescription):
                runners.append(o.runner.name)
        return runners

    def get_target_store(self, runner_name: str) -> str:
        """Returns the name of the target store of the given runner."""
        runner_desc = self._get_runner(runner_name)
        return runner_desc.runner.target_store()

    def get_runner_administrator(self, runner_name: str) -> str:
        """Returns the name of the party administrating a runner."""
        self._registry_replica.update()
        for o in self._registry_replica.objects:
            if isinstance(o, RunnerDescription):
                if o.runner.name == runner_name:
                    return o.site.admin.name
        raise RuntimeError('Runner {} not found'.format(runner_name))

    def get_store_administrator(self, store_name: str) -> str:
        """Returns the name of the party administrating a store."""
        store = self._get_store(store_name)
        return store.site.admin.name

    def list_policy_servers(self) -> List[Tuple[str, IPolicyServer]]:
        """List all known policy servers.

        Return:
            A list of all registered policy servers and their
                    namespaces.
        """
        self._registry_replica.update()

        result = list()     # type: List[Tuple[str, IPolicyServer]]
        for o in self._registry_replica.objects:
            if isinstance(o, PolicyServerDescription):
                result.append((o.namespace.name, o.server))
        return result

    def get_asset_location(self, asset_id: str) -> str:
        """Returns the name of the store which stores this asset."""
        return global_registry.get_asset_location(asset_id)

    def retrieve_data(
            self, store_id: str, name: str) -> Tuple[Any, Metadata]:
        """Obtains a data item from a store."""
        store_desc = self._get_store(store_id)
        return store_desc.store.retrieve(name, self._party)

    def submit_job(
            self, runner_id: str,
            job: Job, plan: Plan
            ) -> None:
        """Submits a job for execution to a local runner.

        Args:
            runner_id: The runner to submit to.
            job: The job to submit.
            plan: The plan to execute the workflow to.
        """
        runner_desc = self._get_runner(runner_id)
        return runner_desc.runner.execute_job(job, plan)

    def _get_runner(self, runner_name: str) -> RunnerDescription:
        """Returns the runner with the given name."""
        self._registry_replica.update()
        for o in self._registry_replica.objects:
            if isinstance(o, RunnerDescription):
                if o.runner.name == runner_name:
                    return o
        raise RuntimeError('Runner {} not found'.format(runner_name))

    def _get_store(self, store_name: str) -> AssetStoreDescription:
        """Returns the store with the given name."""
        self._registry_replica.update()
        for o in self._registry_replica.objects:
            if isinstance(o, AssetStoreDescription):
                if o.store.name == store_name:
                    return o
        raise RuntimeError('Store {} not found'.format(store_name))
