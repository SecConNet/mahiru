"""Some global definitions."""
from typing import Dict

from proof_of_concept.asset import Asset
from proof_of_concept.policy import Rule
from proof_of_concept.replication import IReplicationServer
from proof_of_concept.workflow import Job, WorkflowStep


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


class IAssetStore:
    """An interface for asset stores."""
    name = None     # type: str

    def store(self, asset: Asset) -> None:
        """Stores an asset.

        Args:
            asset: asset object to store

        Raises:
            KeyError: If there's already an asset with the asset id.

        """
        raise NotImplementedError()

    def retrieve(self, asset_id: str, requester: str
                 ) -> Asset:
        """Retrieves an asset.

        Args:
            asset_id: ID of the asset to retrieve.
            requester: Name of the party making the request.

        Return:
            The asset object with asset_id.

        Raises:
            KeyError: If no asset with the given id is stored here.

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


IPolicyServer = IReplicationServer[Rule]
