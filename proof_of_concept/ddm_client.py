"""Functionality for connecting to other DDM sites."""
from typing import List, Optional, Tuple

from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey

from proof_of_concept.asset import Asset
from proof_of_concept.definitions import (
        IAssetStore, ILocalWorkflowRunner, IPolicyServer, Plan)
from proof_of_concept.workflow import Job, Workflow
from proof_of_concept.registry import (
        global_registry, RegisteredObject, SiteDescription)
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
            self, name: str, public_key: RSAPublicKey) -> None:
        """Register a party with the Registry.

        Args:
            name: Name of the party.
            public_key: Public key of this party.

        """
        global_registry.register_party(name, public_key)

    def register_site(
            self,
            name: str,
            owner_name: str,
            admin_name: str,
            runner: Optional[ILocalWorkflowRunner] = None,
            store: Optional[IAssetStore] = None,
            namespace: Optional[str] = None,
            policy_server: Optional[IPolicyServer] = None
            ) -> None:
        """Register a site with the Registry.

        Args:
            name: Name of the site.
            owner_name: Name of the owning party.
            admin_name: Name of the administrating party.
            runner: This site's local workflow runner.
            store: This site's asset store.
            namespace: Namespace managed by this site's policy server.
            policy_server: This site's policy server.
        """
        global_registry.register_site(
                name, owner_name, admin_name, runner, store, namespace,
                policy_server)

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
            if isinstance(o, SiteDescription) and o.namespace is not None:
                if o.namespace == namespace:
                    return o.owner.public_key
        raise RuntimeError('Namespace {} not found'.format(namespace))

    def list_runners(self) -> List[str]:
        """Returns a list of id's of available runners."""
        self._registry_replica.update()
        runners = list()    # type: List[str]
        for o in self._registry_replica.objects:
            if isinstance(o, SiteDescription):
                if o.runner is not None:
                    runners.append(o.runner.name)
        return runners

    def get_target_store(self, runner_name: str) -> str:
        """Returns the name of the target store of the given runner."""
        runner = self._get_runner(runner_name)
        return runner.target_store()

    def get_runner_administrator(self, runner_name: str) -> str:
        """Returns the name of the party administrating a runner."""
        self._registry_replica.update()
        for o in self._registry_replica.objects:
            if isinstance(o, SiteDescription):
                if o.runner is not None:
                    if o.runner.name == runner_name:
                        return o.admin.name
        raise RuntimeError('Runner {} not found'.format(runner_name))

    def get_store_administrator(self, store_name: str) -> str:
        """Returns the name of the party administrating a store."""
        self._registry_replica.update()
        for o in self._registry_replica.objects:
            if isinstance(o, SiteDescription):
                if o.store is not None:
                    if o.store.name == store_name:
                        return o.admin.name
        raise RuntimeError('Store {} not found'.format(store_name))

    def list_policy_servers(self) -> List[Tuple[str, IPolicyServer]]:
        """List all known policy servers.

        Return:
            A list of all registered policy servers and their
                    namespaces.

        """
        self._registry_replica.update()
        result = list()     # type: List[Tuple[str, IPolicyServer]]
        for o in self._registry_replica.objects:
            if isinstance(o, SiteDescription):
                if o.namespace is not None and o.policy_server is not None:
                    result.append((o.namespace, o.policy_server))
        return result

    @staticmethod
    def get_asset_location(asset_id: str) -> str:
        """Returns the name of the store which stores this asset."""
        return global_registry.get_asset_location(asset_id)

    def retrieve_asset(self, store_id: str, asset_id: str
                       ) -> Asset:
        """Obtains a data item from a store."""
        store = self._get_store(store_id)
        return store.retrieve(asset_id, self._party)

    def submit_job(self, runner_id: str, job: Job, plan: Plan) -> None:
        """Submits a job for execution to a local runner.

        Args:
            runner_id: The runner to submit to.
            job: The job to submit.
            plan: The plan to execute the workflow to.

        """
        runner = self._get_runner(runner_id)
        return runner.execute_job(job, plan)

    def _get_runner(self, runner_name: str) -> ILocalWorkflowRunner:
        """Returns the runner with the given name."""
        self._registry_replica.update()
        for o in self._registry_replica.objects:
            if isinstance(o, SiteDescription):
                if o.runner is not None:
                    if o.runner.name == runner_name:
                        return o.runner
        raise RuntimeError(f'Runner {runner_name} not found')

    def _get_store(self, store_name: str) -> IAssetStore:
        """Returns the store with the given name."""
        self._registry_replica.update()
        for o in self._registry_replica.objects:
            if isinstance(o, SiteDescription):
                if o.store is not None:
                    if o.store.name == store_name:
                        return o.store
        raise RuntimeError('Store {} not found'.format(store_name))
