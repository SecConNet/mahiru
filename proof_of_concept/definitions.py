"""Some global definitions."""
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey

from typing import Dict, Optional

from proof_of_concept.asset import Asset
from proof_of_concept.policy import Rule
from proof_of_concept.replication import IReplicationServer
from proof_of_concept.workflow import Job, WorkflowStep


class Plan:
    """A plan for executing a workflow.

    A plan says which step is to be executed by which site, and where
    the inputs should be obtained from.

    Attributes:
        input_sites (Dict[str, str]): Maps inputs to the site to
                obtain them from.
        step_sites (Dict[WorkflowStep, str]): Maps steps to their
                site's id.

    """
    def __init__(
            self, input_sites: Dict[str, str],
            step_sites: Dict[WorkflowStep, str]
            ) -> None:
        """Create a plan.

        Args:
            input_sites: A map from input names to a site id to get
                    them from.
            step_sites: A map from steps to their site's id.

        """
        self.input_sites = input_sites
        self.step_sites = step_sites

    def __str__(self) -> str:
        """Return a string representation of the object."""
        result = ''
        for inp_name, site_id in self.input_sites.items():
            result += '{} <- {}\n'.format(inp_name, site_id)
        for step, site_id in self.step_sites.items():
            result += '{} -> {}\n'.format(step.name, site_id)
        return result


class IAssetStore:
    """An interface for asset stores."""

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


class PartyDescription:
    """Describes a Party to the rest of the DDM.

    Attributes:
        name: Name of the party.
        public_key: The party's public key for signing rules.

    """
    def __init__(self, name: str, public_key: RSAPublicKey) -> None:
        """Create a PartyDescription.

        Args:
            name: Name of the party.
            public_key: The party's public key for signing rules.
        """
        self.name = name
        self.public_key = public_key


class SiteDescription:
    """Describes a site to the rest of the DDM.

    Attributes:
        name: Name of the site.
        owner_name: Name of the party which owns this site.
        admin_name: Name of the party which administrates this site.
        runner: This site's local workflow runner.
        store: This site's asset store.
        namespace: The namespace managed by this site's policy server.
        policy_server: This site's policy server.

    """
    def __init__(
            self,
            name: str,
            owner_name: str,
            admin_name: str,
            runner: Optional[ILocalWorkflowRunner],
            store: Optional[IAssetStore],
            namespace: Optional[str],
            policy_server: Optional[IPolicyServer]
            ) -> None:
        """Create a SiteDescription.

        Args:
            name: Name of the site.
            owner_name: Name of the party which owns this site.
            admin_name: Name of the party which administrates this site.
            runner: This site's local workflow runner.
            store: This site's asset store.
            namespace: The namespace managed by this site's policy
                server.
            policy_server: This site's policy server.

        """
        self.name = name
        self.owner_name = owner_name
        self.admin_name = admin_name
        self.runner = runner
        self.store = store
        self.namespace = namespace
        self.policy_server = policy_server

        if store is None and runner is not None:
            raise RuntimeError('Site with runner needs a store')

        if namespace is None and policy_server is not None:
            raise RuntimeError('Policy server specified without namespace')

        if namespace is not None and policy_server is None:
            raise RuntimeError('Namespace specified but policy server missing')
