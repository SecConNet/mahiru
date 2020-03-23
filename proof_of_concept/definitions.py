"""Some global definitions."""
from typing import Any, Dict, Tuple

from proof_of_concept.workflow import Job, Workflow, WorkflowStep


class Plan:
    """A plan for executing a workflow.

    A plan says which step is to be executed by which runner (i.e. at
    which site), and where the inputs should be obtained from.

    Attributes:
        input_stores (Dict[str, str]): Maps inputs to the store to
                obtain them from.
        step_runners (Dict[WorkflowStep, str]): Maps steps to their
                runner's id.
    """
    def __init__(
            self, input_stores: Dict[str, str],
            step_runners: Dict[WorkflowStep, str]
            ) -> None:
        """Create a plan.

        Args:
            input_stores: A map from input names to a store id to get
                    them from.
            step_runners: A map from steps to their runner's id.
        """
        self.input_stores = input_stores
        self.step_runners = step_runners

    def __str__(self) -> str:
        """Return a string representation of the object."""
        result = ''
        for inp_name, store_id in self.input_stores.items():
            result += '{} <- {}\n'.format(inp_name, store_id)
        for step, runner_id in self.step_runners.items():
            result += '{} -> {}\n'.format(step.name, runner_id)
        return result


class Metadata:
    """Stores metadata for stored assets.

    Attributes:
        job (Job): A minimal job that will generate this asset.
        item (str): The item in the job's workflow corresponding to
                this asset.
    """
    def __init__(self, job: Job, item: str) -> None:
        """Create a Metadata object.

        Args:
            job: A minimal job that will generate this asset.
            item: The item in the job's workflow corresponding to this
                    asset.
        """
        self.job = job
        self.item = item


class IAssetStore:
    """An interface for asset stores."""
    name = None     # type: str

    def store(self, name: str, data: Any, metadata: Metadata) -> None:
        """Stores an asset.

        Args:
            name: Name to store asset under.
            data: Asset data to store.
            metadata: Metadata to annotate the asset with.

        Raises:
            KeyError: If there's already an asset with name ``name``.
        """
        raise NotImplementedError()

    def retrieve(
            self, asset_name: str, requester: str) -> Tuple[Any, Metadata]:
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
    """Interface for services for running workflows at a given site."""
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
