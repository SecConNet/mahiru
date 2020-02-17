from typing import Any, Dict

from workflow import Workflow, WorkflowStep


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

    def retrieve(self, name: str) -> Any:
        """Retrieves an asset.

        Args:
            name: Name of the asset to retrieve.

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

    def execute_plan(
            self,
            workflow: Workflow, inputs: Dict[str, str], plan: Plan
            ) -> None:
        """Executes the local part of a plan.

        This runs any steps in the given workflow which are to be
        executed by this runner according to the given plan.

        Args:
            workflow: The workflow to execute part of.
            inputs: Inputs for the workflow.
            plan: The plan according to which to execute.
        """
        raise NotImplementedError()
