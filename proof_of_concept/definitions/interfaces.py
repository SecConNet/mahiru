"""Widely used interface definitions."""
from datetime import datetime
from pathlib import Path
from typing import Dict, Generic, Iterable, Set, Type, TypeVar

from proof_of_concept.definitions.identifier import Identifier
from proof_of_concept.definitions.assets import Asset, ComputeAsset
from proof_of_concept.definitions.policy import Rule
from proof_of_concept.definitions.registry import (
        PartyDescription, RegisteredObject, SiteDescription)
from proof_of_concept.definitions.workflows import (
        ExecutionRequest, Job, WorkflowStep)


T = TypeVar('T')


class IReplicaUpdate(Generic[T]):
    """Contains an update for a Replica.

    Attributes:
        from_version: Version to apply this update to.
        to_version: Version this update updates to.
        valid_until: Time until which the new version is valid.
        created: Set of objects that were created.
        deleted: Set of objects that were deleted.
    """
    ReplicatedType = None      # type: Type[T]

    from_version = None     # type: int
    to_version = None       # type: int
    valid_until = None      # type: datetime
    created = None          # type: Set[T]
    deleted = None          # type: Set[T]


class IReplicationService(Generic[T]):
    """Generic interface for replication sources."""
    def get_updates_since(self, from_version: int) -> IReplicaUpdate[T]:
        """Return a set of objects modified since the given version.

        Args:
            from_version: A version received from a previous call to
                    this function, or 0 to get an update for a
                    fresh replica.

        Return:
            An update from the given version to a newer version.
        """
        raise NotImplementedError()


class IRegistry(IReplicationService[RegisteredObject]):
    """Interface for the central registry.

    This is equivalent to the registry REST API, and is implemented
    by the Registry class and the RegistryRestClient class.
    """
    def register_party(
            self, description: PartyDescription) -> None:
        """Register a party with the DDM.

        Args:
            description: A description of the party
        """
        raise NotImplementedError()

    def deregister_party(self, party_id: Identifier) -> None:
        """Deregister a party with the DDM.

        Args:
            party_id: Identifier of the party to deregister.
        """
        raise NotImplementedError()

    def register_site(self, description: SiteDescription) -> None:
        """Register a Site with the Registry.

        Args:
            description: Description of the site.

        """
        raise NotImplementedError()

    def deregister_site(self, site_id: Identifier) -> None:
        """Deregister a site with the DDM.

        Args:
            site_id: Identifer of the site to deregister.
        """
        raise NotImplementedError()


class IPolicyCollection:
    """Provides policies to a PolicyEvaluator."""
    def policies(self) -> Iterable[Rule]:
        """Returns an iterable collection of rules."""
        raise NotImplementedError()


class IAssetStore:
    """An interface for asset stores."""

    def store(self, asset: Asset, move_image: bool = False) -> None:
        """Stores an asset.

        Args:
            asset: asset object to store
            move_image: If the asset has an image and True is passed,
                the image file will be moved rather than copied into
                the store.

        Raises:
            KeyError: If there's already an asset with the asset id.

        """
        raise NotImplementedError()

    def store_image(
            self, asset_id: Identifier, image_file: Path,
            move_image: bool = False) -> None:
        """Stores an image for an already-stored asset.

        Args:
            asset_id: ID of the asset to add an image for.
            image_file: Path to the file to store.
            move_image: If True, move the image instead of copying it.

        """
        raise NotImplementedError()

    def retrieve(self, asset_id: Identifier, requester: str
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

    def execute_request(
            self,
            request: ExecutionRequest
            ) -> None:
        """Executes the local part of a plan.

        This runs any steps in the given workflow which are to be
        executed by this runner according to the given plan.

        Args:
            request: the job to execute and plan to do it.

        """
        raise NotImplementedError()


class IDomainAdministrator:
    """Manages container and network resources for a site.

    The "domain" in the name is a system administration or networking
    domain, containing (virtual) networks and containers (and
    potentially virtual machines, (virtual) programmable networking
    hardware, etc. in which workflows are executed. Classes
    implementing this interface manage ("administrate") these resources
    in the domain to implement workflow execution.
    """
    def execute_step(
            self, step: WorkflowStep, inputs: Dict[str, Asset],
            compute_asset: ComputeAsset, id_hashes: Dict[str, str],
            step_subjob: Job) -> None:
        """Execute the given workflow step.

        Args:
            step: The step to execute.
            inputs: Input assets indexed by input name.
            compute_asset: The compute asset to run.
            id_hashes: A hash for each workflow item, indexed by its
                name.
            step_subjob: A subjob for the step's results' metadata.
        """
        raise NotImplementedError()
