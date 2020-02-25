from typing import Any, Dict, List, Optional, Tuple

from definitions import IAssetStore, ILocalWorkflowRunner, Plan
from workflow import Job, Workflow
from registry import global_registry


class DDMClient:
    """Handles connecting to global registry, runners and stores.
    """
    def __init__(self) -> None:
        pass

    def register_runner(
            self, admin: str, runner: ILocalWorkflowRunner
            ) -> None:
        """Register a LocalWorkflowRunner with the Registry.

        Args:
            admin: The party administrating this runner.
            runner: The runner to register.
        """
        global_registry.register_runner(admin, runner)

    def register_store(self, store: IAssetStore) -> None:
        """Register a AssetStore with the Registry.

        Args:
            store: The data store to register.
        """
        global_registry.register_store(store)

    def list_runners(self) -> List[str]:
        """Returns a list of id's of available runners.
        """
        return global_registry.list_runners()

    def get_target_store(self, runner_id: str) -> str:
        """Returns the id of the target store of the given runner.
        """
        return global_registry.get_runner(runner_id).target_store()

    def get_runner_administrator(self, runner_id: str) -> str:
        """Returns the id of the party administrating a runner.
        """
        return global_registry.get_runner_admin(runner_id)

    def retrieve_data(
            self, store_id: str, name: str) -> Tuple[Any, Job]:
        """Obtains a data item from a store.
        """
        store = global_registry.get_store(store_id)
        return store.retrieve(name)

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
