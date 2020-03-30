"""Functionality for connecting to other DDM sites."""
from typing import Any, Dict, List, Optional, Tuple

from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey

from proof_of_concept.definitions import (
        IAssetStore, ILocalWorkflowRunner, IPolicyServer, Metadata, Plan)
from proof_of_concept.workflow import Job, Workflow
from proof_of_concept.registry import global_registry


class DDMClient:
    """Handles connecting to global registry, runners and stores."""
    def __init__(self, party: str) -> None:
        """Create a DDMClient.

        Args:
            party: The party on whose behalf this client acts.
        """
        self._party = party

    def register_party(
            self, name: str, namespace: str, public_key: RSAPublicKey) -> None:
        """Register a party with the Registry.

        Args:
            name: Name of the party.
            namespace: ID namespace owned by this party.
            public_key: Public key of this party.
        """
        global_registry.register_party(name, namespace, public_key)

    def register_runner(
            self, admin: str, runner: ILocalWorkflowRunner
            ) -> None:
        """Register a LocalWorkflowRunner with the Registry.

        Args:
            admin: The party administrating this runner.
            runner: The runner to register.
        """
        global_registry.register_runner(admin, runner)

    def register_store(self, admin: str, store: IAssetStore) -> None:
        """Register a AssetStore with the Registry.

        Args:
            admin: The party administrating this runner.
            store: The data store to register.
        """
        global_registry.register_store(admin, store)

    def register_policy_server(
            self, admin: str, server: IPolicyServer) -> None:
        """Register a policy server with the registry.

        Args:
            admin: The party administrating this runner.
            server: The data store to register.
        """
        global_registry.register_policy_server(admin, server)

    def register_asset(self, asset_id: str, store_name: str) -> None:
        """Register an Asset with the Registry.

        Args:
            asset_id: The id of the asset to register.
            store_name: Name of the store where it can be found.
        """
        global_registry.register_asset(asset_id, store_name)

    def get_public_key_for_ns(self, namespace: str) -> RSAPublicKey:
        """Get the public key of the owner of a namespace."""
        owner = global_registry.get_ns_owner(namespace)
        return global_registry.get_public_key(owner)

    def list_runners(self) -> List[str]:
        """Returns a list of id's of available runners."""
        return global_registry.list_runners()

    def get_target_store(self, runner_id: str) -> str:
        """Returns the id of the target store of the given runner."""
        return global_registry.get_runner(runner_id).target_store()

    def get_runner_administrator(self, runner_id: str) -> str:
        """Returns the id of the party administrating a runner."""
        return global_registry.get_runner_admin(runner_id)

    def get_store_administrator(self, store_id: str) -> str:
        """Returns the id of the party administrating a store."""
        return global_registry.get_store_admin(store_id)

    def list_policy_servers(self) -> List[IPolicyServer]:
        """List all known policy servers.

        Return:
            A list of all registered policy servers.
        """
        return global_registry.list_policy_servers()

    def get_asset_location(self, asset_id: str) -> str:
        """Returns the name of the store which stores this asset."""
        return global_registry.get_asset_location(asset_id)

    def retrieve_data(
            self, store_id: str, name: str) -> Tuple[Any, Metadata]:
        """Obtains a data item from a store."""
        store = global_registry.get_store(store_id)
        return store.retrieve(name, self._party)

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
        runner = global_registry.get_runner(runner_id)
        return runner.execute_job(job, plan)
