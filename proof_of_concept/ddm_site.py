"""This module combines components into a site installation."""
from typing import Any, Dict, List

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa

from proof_of_concept.asset import Asset
from proof_of_concept.asset_store import AssetStore
from proof_of_concept.ddm_client import DDMClient
from proof_of_concept.local_workflow_runner import LocalWorkflowRunner
from proof_of_concept.policy import PolicyEvaluator, Rule
from proof_of_concept.policy_replication import PolicySource
from proof_of_concept.replication import (
    CanonicalStore, ReplicableArchive, ReplicationServer)
from proof_of_concept.workflow import Job
from proof_of_concept.workflow_engine import GlobalWorkflowRunner


class Site:
    """Represents a single DDM peer installation."""
    def __init__(
            self, name: str, administrator: str,
            namespace: str, stored_data: List[Asset],
            rules: List[Rule]) -> None:
        """Create a Site.

        Also registers its runner and store in the global registry.

        Args:
            name: Name of the site
            administrator: Party which administrates this site.
            namespace: Namespace used by this site.
            stored_data: Data sets stored at this site.
            rules: A policy to adhere to.

        """
        # Metadata
        self.name = name
        self.administrator = administrator
        self.namespace = namespace

        self._ddm_client = DDMClient(administrator)

        # Register party with DDM
        self._private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=2048,
                backend=default_backend())

        self._ddm_client.register_party(
                self.administrator, self.namespace,
                self._private_key.public_key())

        # Register site with DDM
        self._ddm_client.register_site(self.name + '-site', administrator)

        # Policy support
        self._policy_archive = ReplicableArchive[Rule]()
        self._policy_store = CanonicalStore[Rule](self._policy_archive)
        for rule in rules:
            rule.sign(self._private_key)
            self._policy_store.insert(rule)
        self.policy_server = ReplicationServer[Rule](
                self._policy_archive, 10.0)
        self._ddm_client.register_policy_server(
                self.name + '-site', self.namespace, self.policy_server)

        self._policy_source = PolicySource(
                self._ddm_client, self._policy_store)
        self._policy_evaluator = PolicyEvaluator(self._policy_source)

        # Server side
        self.store = AssetStore(name + '-store', self._policy_evaluator)
        self._ddm_client.register_store(self.name + '-site', self.store)
        for asset in stored_data:
            self.store.store(asset)
            self._ddm_client.register_asset(asset.id, self.store.name)

        self.runner = LocalWorkflowRunner(
                name + '-runner', administrator,
                self._policy_evaluator, self.store)
        self._ddm_client.register_runner(
               self.name + '-site', administrator, self.runner)

        # Client side
        self._workflow_engine = GlobalWorkflowRunner(
                self._policy_evaluator, self._ddm_client)

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        return 'Site({})'.format(self.name)

    def run_job(self, job: Job) -> Dict[str, Any]:
        """Run a workflow on behalf of the party running this site."""
        return self._workflow_engine.execute(self.administrator, job)
