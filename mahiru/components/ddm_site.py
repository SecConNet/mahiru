"""A site installation."""
import logging
from typing import Any, Dict, List

import ruamel.yaml as yaml

from mahiru.components.asset_store import AssetStore
from mahiru.components.domain_administrator import PlainDockerDA
from mahiru.components.network_administrator import WireGuardNA
from mahiru.components.registry_client import RegistryClient
from mahiru.components.settings import SiteConfiguration
from mahiru.components.step_runner import StepRunner
from mahiru.definitions.assets import Asset
from mahiru.definitions.identifier import Identifier
from mahiru.definitions.policy import Rule
from mahiru.definitions.workflows import Job
from mahiru.rest.site_client import SiteRestClient
from mahiru.policy.evaluation import PolicyEvaluator
from mahiru.policy.replication import PolicyStore
from mahiru.replication import ReplicableArchive
from mahiru.components.orchestration import WorkflowOrchestrator
from mahiru.components.policy_client import PolicyClient


logger = logging.getLogger(__name__)


class Site:
    """Represents a single DDM installation."""
    def __init__(
            self, config: SiteConfiguration, stored_data: List[Asset],
            rules: List[Rule], registry_client: RegistryClient) -> None:
        """Create a Site.

        Args:
            config: Configuration for the site.
            stored_data: Data sets stored at this site.
            rules: A policy to adhere to.
            registry_client: A RegistryClient to use.

        """
        # Metadata
        self.id = Identifier(f'site:{config.namespace}:{config.name}')
        self.owner = config.owner
        # Owner and administrator are the same for now, but could
        # in principle be different, e.g. in a SaaS scenario. They also
        # differ semantically, so we have both here to make that clear.
        self.administrator = config.owner
        self.namespace = config.namespace

        # Create clients for talking to the DDM
        self._registry_client = registry_client
        self._site_rest_client = SiteRestClient(self.id, self._registry_client)

        # Policy support
        self._policy_archive = ReplicableArchive[Rule]()
        self.policy_store = PolicyStore(self._policy_archive, 0.1)
        for rule in rules:
            self.policy_store.insert(rule)

        self._policy_client = PolicyClient(self._registry_client)
        self._policy_evaluator = PolicyEvaluator(self._policy_client)

        # Server side
        self._network_administrator = WireGuardNA(
                config.network_settings, self._site_rest_client)

        self._domain_administrator = PlainDockerDA(
                self._network_administrator, self._site_rest_client)

        self.store = AssetStore(self._policy_evaluator)

        self.runner = StepRunner(
                self.id, self._site_rest_client, self._policy_evaluator,
                self._domain_administrator, self.store)

        # Client side
        self.orchestrator = WorkflowOrchestrator(
                self._policy_evaluator, self._registry_client,
                self._site_rest_client)

        # Insert data
        for asset in stored_data:
            self.store.store(asset)

    def close(self) -> None:
        """Release resources."""
        self.store.close()

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        return 'Site({})'.format(self.id)

    def run_job(self, job: Job) -> Dict[str, Asset]:
        """Run a workflow on behalf of the party running this site."""
        logger.info('Starting job execution')
        job_id = self.orchestrator.start_job(self.id, job)
        return self.orchestrator.get_results(job_id)
