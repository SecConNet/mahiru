"""This module combines components into a site installation."""
import logging
from typing import Any, Dict, List

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa

from proof_of_concept.asset import Asset
from proof_of_concept.asset_store import AssetStore
from proof_of_concept.ddm_client import DDMClient
from proof_of_concept.definitions import PartyDescription
from proof_of_concept.local_workflow_runner import LocalWorkflowRunner
from proof_of_concept.policy import PolicyEvaluator, Rule
from proof_of_concept.policy_replication import PolicySource
from proof_of_concept.replication import (
    CanonicalStore, ReplicableArchive, ReplicationServer)
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

        self._ddm_client = DDMClient(self.administrator)

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
        self.policy_server = ReplicationServer[Rule](
                self._policy_archive, 10.0)

        self._policy_source = PolicySource(
                self._ddm_client, self._policy_store)
        self._policy_evaluator = PolicyEvaluator(self._policy_source)

        # Server side
        self.store = AssetStore(self._policy_evaluator)

        self.runner = LocalWorkflowRunner(
                name, self.administrator,
                self._policy_evaluator, self.store)

        # Client side
        self._workflow_engine = GlobalWorkflowRunner(
                self._policy_evaluator, self._ddm_client)

        # Register site with DDM
        self._ddm_client.register_site(
                self.name, self.owner, self.administrator,
                self.runner, self.store, self.namespace, self.policy_server)

        # Insert data
        for asset in stored_data:
            self.store.store(asset)
            self._ddm_client.register_asset(asset.id, self.name)

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        return 'Site({})'.format(self.name)

    def run_job(self, job: Job) -> Dict[str, Any]:
        """Run a workflow on behalf of the party running this site."""
        logger.info('Starting job execution')
        return self._workflow_engine.execute(self.administrator, job)
