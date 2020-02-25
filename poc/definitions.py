from typing import Any, Dict

from workflow import Job, Workflow, WorkflowStep


Plan = Dict[WorkflowStep, str]      # maps step to LocalWorkflowRunner name


class IAssetStore:
    """An interface for asset stores.
    """
    name = None     # type: str

    def store(self, name: str, data: Any) -> None:
        """Stores an asset.

        Args:
            name: Name to store asset under.
            data: Asset data to store.

        Raises:
            KeyError: If there's already an asset with name ``name``.
        """
        raise NotImplementedError()

    def retrieve(self, asset_name: str, requester: str) -> Any:
        """Retrieves an asset.

        Args:
            asset_name: Name of the asset to retrieve.
            requester: Name of the party making the request.

        Return:
            The asset data stored under the given name.

        Raises:
            KeyError: If no asset with the given name is stored here.
        """
        raise NotImplementedError()


class ILocalWorkflowRunner:
    """A interface for services for running workflows at a given site.
    """
    name = None     # type: str

    def target_store(self) -> str:
        """Returns the name of the store containing our results.

        Returns:
            A string with the name.
        """
        raise NotImplementedError()

    def execute_job(
            self,
            job: Job, plan: Plan
            ) -> None:
        """Executes the local part of a plan.

        This runs any steps in the given workflow which are to be
        executed by this runner according to the given plan.

        Args:
            job: The job to execute part of.
            plan: The plan according to which to execute.
        """
        raise NotImplementedError()
