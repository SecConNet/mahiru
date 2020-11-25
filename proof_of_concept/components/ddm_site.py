"""A site installation."""
import logging
from pathlib import Path
from typing import Any, Dict, List

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
import ruamel.yaml as yaml

from proof_of_concept.components.asset_store import AssetStore
from proof_of_concept.components.registry_client import RegistryClient
from proof_of_concept.definitions.assets import Asset
from proof_of_concept.definitions.policy import Rule
from proof_of_concept.definitions.registry import (
        PartyDescription, SiteDescription)
from proof_of_concept.definitions.workflows import Job
from proof_of_concept.rest.client import SiteRestClient
from proof_of_concept.rest.ddm_site import SiteRestApi, SiteServer
from proof_of_concept.components.step_runner import StepRunner
from proof_of_concept.policy.evaluation import PolicyEvaluator
from proof_of_concept.policy.replication import PolicyStore
from proof_of_concept.replication import ReplicableArchive
from proof_of_concept.rest.validation import Validator
from proof_of_concept.components.orchestration import WorkflowOrchestrator
from proof_of_concept.components.policy_client import PolicyClient


logger = logging.getLogger(__file__)


class Site:
    """Represents a single DDM installation."""
    def __init__(
            self, name: str, owner: str,
            namespace: str, stored_data: List[Asset],
            rules: List[Rule]) -> None:
        """Create a Site.

        Also registers its runner and store in the global registry.

        Args:
            name: Name of the site
            owner: Party which owns this site.
            namespace: Namespace used by this site.
            stored_data: Data sets stored at this site.
            rules: A policy to adhere to.

        """
        # Metadata
        self.name = name
        self.owner = owner
        # Owner and administrator are the same for now, but could
        # in principle be different, e.g. in a SaaS scenario. They also
        # differ semantically, so we have both here to make that clear.
        self.administrator = owner
        self.namespace = namespace

        # Load API definitions
        site_api_file = Path(__file__).parents[1] / 'rest' / 'site_api.yaml'
        with open(site_api_file, 'r') as f:
            site_api_def = yaml.safe_load(f.read())

        self._site_validator = Validator(site_api_def)

        # Create clients for talking to the DDM
        self._registry_client = RegistryClient()
        self._site_rest_client = SiteRestClient(
                self.name, self._site_validator, self._registry_client)

        # Register party with DDM
        self._private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=2048,
                backend=default_backend())

        self._registry_client.register_party(
                PartyDescription(
                    self.administrator, self._private_key.public_key()))

        # Policy support
        self._policy_archive = ReplicableArchive[Rule]()
        self._policy_store = PolicyStore(self._policy_archive, 0.1)
        for rule in rules:
            rule.sign(self._private_key)
            self._policy_store.insert(rule)

        self._policy_client = PolicyClient(
                self._registry_client, self._site_validator)
        self._policy_evaluator = PolicyEvaluator(self._policy_client)

        # Server side
        self.store = AssetStore(self._policy_evaluator)

        self.runner = StepRunner(
                name, self._registry_client, self._site_rest_client,
                self._policy_evaluator, self.store)

        # REST server
        self.api = SiteRestApi(self._policy_store, self.store, self.runner)
        self.server = SiteServer(self.api)

        # Client side
        self._workflow_engine = WorkflowOrchestrator(
                self._policy_evaluator, self._registry_client,
                self._site_rest_client)

        # Register site with DDM
        self._registry_client.register_site(
                SiteDescription(
                    self.name, self.owner, self.administrator,
                    self.server.endpoint, True, True, self.namespace))

        # Insert data
        for asset in stored_data:
            self.store.store(asset)

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        return 'Site({})'.format(self.name)

    def close(self) -> None:
        """Shut down the site."""
        self._registry_client.deregister_site(self.name)
        self._registry_client.deregister_party(self.administrator)
        self.server.close()

    def run_job(self, job: Job) -> Dict[str, Any]:
        """Run a workflow on behalf of the party running this site."""
        logger.info('Starting job execution')
        return self._workflow_engine.execute(self.name, job)
