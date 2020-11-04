"""This module combines components into a site installation."""
import logging
from pathlib import Path
from typing import Any, Dict, List

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
import ruamel.yaml as yaml

from proof_of_concept.asset import Asset
from proof_of_concept.asset_store import AssetStore
from proof_of_concept.ddm_client import DDMClient
from proof_of_concept.ddm_site_api import SiteApi, SiteServer
from proof_of_concept.definitions import PartyDescription, SiteDescription
from proof_of_concept.local_workflow_runner import LocalWorkflowRunner
from proof_of_concept.policy import PolicyEvaluator, Rule
from proof_of_concept.policy_replication import PolicyServer, PolicySource
from proof_of_concept.replication import CanonicalStore, ReplicableArchive
from proof_of_concept.validation import Validator
from proof_of_concept.workflow import Job
from proof_of_concept.workflow_engine import GlobalWorkflowRunner


logger = logging.getLogger(__file__)


class Site:
    """Represents a single DDM peer installation."""
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
        site_api_file = Path(__file__).parent / 'site_api.yaml'
        with open(site_api_file, 'r') as f:
            site_api_def = yaml.safe_load(f.read())

        self._site_validator = Validator(site_api_def)

        # Create client for talking to other sites
        self._ddm_client = DDMClient(self.name, self._site_validator)

        # Register party with DDM
        self._private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=2048,
                backend=default_backend())

        self._ddm_client.register_party(
                PartyDescription(
                    self.administrator, self._private_key.public_key()))

        # Policy support
        self._policy_archive = ReplicableArchive[Rule]()
        self._policy_store = CanonicalStore[Rule](self._policy_archive)
        for rule in rules:
            rule.sign(self._private_key)
            self._policy_store.insert(rule)
        self.policy_server = PolicyServer(self._policy_archive, 0.1)

        self._policy_source = PolicySource(self._ddm_client)
        self._policy_evaluator = PolicyEvaluator(self._policy_source)

        # Server side
        self.store = AssetStore(self._policy_evaluator)

        self.runner = LocalWorkflowRunner(
                name, self._ddm_client, self._policy_evaluator, self.store)

        # REST server
        self.api = SiteApi(self.policy_server, self.store, self.runner)
        self.server = SiteServer(self.api)

        # Client side
        self._workflow_engine = GlobalWorkflowRunner(
                self._policy_evaluator, self._ddm_client)

        # Register site with DDM
        self._ddm_client.register_site(
                SiteDescription(
                    self.name, self.owner, self.administrator,
                    self.server.endpoint, True, True, self.namespace))

        # Insert data
        for asset in stored_data:
            self.store.store(asset)
            self._ddm_client.register_asset(asset.id, self.name)

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        return 'Site({})'.format(self.name)

    def close(self) -> None:
        """Shut down the site."""
        self._ddm_client.deregister_site(self.name)
        self._ddm_client.deregister_party(self.administrator)
        self.server.close()

    def run_job(self, job: Job) -> Dict[str, Any]:
        """Run a workflow on behalf of the party running this site."""
        logger.info('Starting job execution')
        return self._workflow_engine.execute(self.name, job)
