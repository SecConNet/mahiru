"""This module combines components into a site installation."""
from typing import Any, Dict, List

from proof_of_concept.asset_store import AssetStore
from proof_of_concept.ddm_client import DDMClient
from proof_of_concept.definitions import Metadata
from proof_of_concept.local_workflow_runner import LocalWorkflowRunner
from proof_of_concept.policy import PolicyManager, Rule
from proof_of_concept.workflow import Job, Workflow
from proof_of_concept.workflow_engine import GlobalWorkflowRunner


class Site:
    """Represents a single DDM peer installation."""
    def __init__(
            self, name: str, administrator: str, stored_data: Dict[str, int],
            rules: List[Rule]) -> None:
        """Create a Site.

        Also registers its runner and store in the global registry.

        Args:
            name: Name of the site
            administrator: Party which administrates this site.
            stored_data: Data sets stored at this site.
            rules: A policy to adhere to.
        """
        # Metadata
        self.name = name
        self.administrator = administrator

        self._ddm_client = DDMClient(administrator)
        self._policy_manager = PolicyManager(rules)

        # Server side
        self.store = AssetStore(name + '-store', self._policy_manager)
        self._ddm_client.register_store(administrator, self.store)
        for key, val in stored_data.items():
            self.store.store(key, val, Metadata(Job.niljob(key), 'dataset'))
            self._ddm_client.register_asset(key, self.store.name)

        self.runner = LocalWorkflowRunner(
                name + '-runner', administrator,
                self._policy_manager, self.store)
        self._ddm_client.register_runner(administrator, self.runner)

        # Client side
        self._workflow_engine = GlobalWorkflowRunner(
                self._policy_manager, self._ddm_client)

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        return 'Site({})'.format(self.name)

    def run_job(self, job: Job) -> Dict[str, Any]:
        """Run a workflow on behalf of the party running this site."""
        return self._workflow_engine.execute(self.administrator, job)
