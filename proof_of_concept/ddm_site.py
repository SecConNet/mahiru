"""This module combines components into a site installation."""
from typing import Any, Dict, Iterable, List

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa

from proof_of_concept.asset_store import AssetStore
from proof_of_concept.ddm_client import DDMClient
from proof_of_concept.definitions import Metadata
from proof_of_concept.local_workflow_runner import LocalWorkflowRunner
from proof_of_concept.policy import IPolicySource, PolicyEvaluator, Rule
from proof_of_concept.replication import (
        CanonicalStore, IReplicationServer, Replica, ReplicableArchive,
        ReplicationServer)
from proof_of_concept.workflow import Job, Workflow
from proof_of_concept.workflow_engine import GlobalWorkflowRunner


class PolicySource(IPolicySource):
    """Ties together various sources of policies."""
    def __init__(
            self, ddm_client: DDMClient, our_store: CanonicalStore[Rule]
            ) -> None:
        """Create a PolicySource.

        This will automatically keep the replicas up-to-date as needed.

        Args:
            ddm_client: A DDMClient to use for getting servers.
            our_store: A store containing our policies.
        """
        self._ddm_client = ddm_client
        self._our_store = our_store
        OtherStores = Dict[IReplicationServer[Rule], Replica[Rule]]
        self._other_stores = dict()     # type: OtherStores

    def policies(self) -> Iterable[Rule]:
        """Returns the collected rules."""
        self._update()
        our_rules = list(self._our_store.objects())
        their_rules = [
                rule for store in self._other_stores.values()
                for rule in store.objects]
        return our_rules + their_rules

    def _update(self) -> None:
        """Update sources to match the given set."""
        new_servers = self._ddm_client.list_policy_servers()
        # add new servers
        for new_server in new_servers:
            if new_server not in self._other_stores:
                self._other_stores[new_server] = Replica[Rule](new_server)

        # removed ones that disappeared
        removed_servers = [
                server for server in self._other_stores
                if server not in new_servers]

        for server in removed_servers:
            del(self._other_stores[server])

        # update everyone
        for store in self._other_stores.values():
            store.update()


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

        # Register party with DDM
        self._private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=2048,
                backend=default_backend())

        self._ddm_client.register_party(
                self.name, self.name, self._private_key.public_key())

        # Policy support
        self._policy_archive = ReplicableArchive[Rule]()
        self._policy_store = CanonicalStore[Rule](self._policy_archive)
        for rule in rules:
            self._policy_store.insert(rule)
        self.policy_server = ReplicationServer[Rule](
                self._policy_archive, 10.0)
        self._ddm_client.register_policy_server(self.name, self.policy_server)

        self._policy_source = PolicySource(
                self._ddm_client, self._policy_store)
        self._policy_evaluator = PolicyEvaluator(self._policy_source)

        # Server side
        self.store = AssetStore(name + '-store', self._policy_evaluator)
        self._ddm_client.register_store(administrator, self.store)
        for key, val in stored_data.items():
            self.store.store(key, val, Metadata(Job.niljob(key), 'dataset'))
            self._ddm_client.register_asset(key, self.store.name)

        self.runner = LocalWorkflowRunner(
                name + '-runner', administrator,
                self._policy_evaluator, self.store)
        self._ddm_client.register_runner(administrator, self.runner)

        # Client side
        self._workflow_engine = GlobalWorkflowRunner(
                self._policy_evaluator, self._ddm_client)

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        return 'Site({})'.format(self.name)

    def run_job(self, job: Job) -> Dict[str, Any]:
        """Run a workflow on behalf of the party running this site."""
        return self._workflow_engine.execute(self.administrator, job)
