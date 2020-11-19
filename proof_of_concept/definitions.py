"""Some global definitions."""
from datetime import datetime
from typing import Any, Dict, Generic, Optional, Set, Type, TypeVar, Union

from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey

from proof_of_concept.asset import Asset
from proof_of_concept.policy import Rule
from proof_of_concept.workflow import Job, WorkflowStep


JSON = Dict[str, Any]


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
            step_sites: Dict[str, str]
            ) -> None:
        """Create a plan.

        Args:
            input_sites: A map from input names to a site id to get
                    them from.
            step_sites: A map from step names to their site's id.

        """
        self.input_sites = input_sites
        self.step_sites = step_sites

    def __str__(self) -> str:
        """Return a string representation of the object."""
        result = ''
        for inp_name, site_id in self.input_sites.items():
            result += '{} <- {}\n'.format(inp_name, site_id)
        for step_name, site_id in self.step_sites.items():
            result += '{} -> {}\n'.format(step_name, site_id)
        return result


class JobSubmission:
    """A submission of a job and execution plan to a site.

    Attributes:
        job: The job we're executing.
        plan: The plan according to which it should be executed.

    """
    def __init__(self, job: Job, plan: Plan) -> None:
        """Create a JobSubmission.

        Args:
            job: The job we're executing.
            plan: The plan according to which it should be executed.
        """
        self.job = job
        self.plan = plan


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
            requester: Name of the site making the request.

        Return:
            The asset object with asset_id.

        Raises:
            KeyError: If no asset with the given id is stored here.

        """
        raise NotImplementedError()


class IStepRunner:
    """Interface for services for running steps at a given site."""

    def execute_job(
            self,
            submission: JobSubmission
            ) -> None:
        """Executes the local part of a plan.

        This runs any steps in the given workflow which are to be
        executed by this runner according to the given plan.

        Args:
            submission: the job to execute and plan to do it.

        """
        raise NotImplementedError()


T = TypeVar('T')


class ReplicaUpdate(Generic[T]):
    """Contains an update for a Replica.

    Attributes:
        from_version: Version to apply this update to.
        to_version: Version this update updates to.
        valid_until: Time until which the new version is valid.
        created: Set of objects that were created.
        deleted: Set of objects that were deleted.
    """
    ReplicatedType = None      # type: Type[T]

    def __init__(
            self, from_version: int, to_version: int, valid_until: datetime,
            created: Set[T], deleted: Set[T]) -> None:
        """Create a replica update.

        Args:
            from_version: Version to apply this update to.
            to_version: Version this update updates to.
            valid_until: Point in time until which the new version is
                valid.
            created: Set of objects that were created.
            deleted: Set of objects that were deleted.
        """
        self.from_version = from_version
        self.to_version = to_version
        self.valid_until = valid_until
        self.created = created
        self.deleted = deleted

    def __repr__(self) -> str:
        """Return a string representation of the object."""
        return (
            f'ReplicaUpdate({self.from_version} -> {self.to_version},'
            f' {self.valid_until}, +{self.created}, -{self.deleted})')


class IReplicationService(Generic[T]):
    """Generic interface for replication sources."""
    def get_updates_since(self, from_version: int) -> ReplicaUpdate[T]:
        """Return a set of objects modified since the given version.

        Args:
            from_version: A version received from a previous call to
                    this function, or 0 to get an update for a
                    fresh replica.

        Return:
            An update from the given version to a newer version.
        """
        raise NotImplementedError()


class RegisteredObject:
    """Base class for objects in the registry."""
    pass


class PartyDescription(RegisteredObject):
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

    def __repr__(self) -> str:
        """Returns a string representation of the object."""
        return f'PartyDescription({self.name})'


class SiteDescription(RegisteredObject):
    """Describes a site to the rest of the DDM.

    Attributes:
        name: Name of the site.
        owner_name: Name of the party which owns this site.
        admin_name: Name of the party which administrates this site.
        endpoint: This site's REST endpoint.
        runner: Whether the site has a runner.
        store: Whether the site has a store.
        namespace: The namespace managed by this site's policy server,
            if any.

    """
    def __init__(
            self,
            name: str,
            owner_name: str,
            admin_name: str,
            endpoint: str,
            runner: bool,
            store: bool,
            namespace: Optional[str]
            ) -> None:
        """Create a SiteDescription.

        Args:
            name: Name of the site.
            owner_name: Name of the party which owns this site.
            admin_name: Name of the party which administrates this site.
            endpoint: URL of the REST endpoint of this site.
            runner: Whether the site has a runner.
            store: Whether the site has a store.
            namespace: The namespace managed by this site's policy
                server, if any.

        """
        self.name = name
        self.owner_name = owner_name
        self.admin_name = admin_name
        self.endpoint = endpoint
        self.runner = runner
        self.store = store
        self.namespace = namespace

        if runner and not store:
            raise RuntimeError('Site with runner needs a store')

    def __repr__(self) -> str:
        """Returns a string representation of the object."""
        return f'SiteDescription({self.name})'


class PolicyUpdate(ReplicaUpdate[Rule]):
    """An update for policy replicas."""
    ReplicatedType = Rule


class RegistryUpdate(ReplicaUpdate[RegisteredObject]):
    """An update for registry replicas."""
    ReplicatedType = RegisteredObject
