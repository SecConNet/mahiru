"""Widely used interface definitions."""
from datetime import datetime
from pathlib import Path
from typing import Dict, Generic, Iterable, Set, Tuple, Type, TypeVar

from mahiru.definitions.connections import ConnectionInfo, ConnectionRequest
from mahiru.definitions.identifier import Identifier
from mahiru.definitions.assets import Asset, ComputeAsset, DataAsset
from mahiru.definitions.policy import Rule
from mahiru.definitions.registry import (
        PartyDescription, RegisteredObject, SiteDescription)
from mahiru.definitions.workflows import (
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


IRegistryService = IReplicationService[RegisteredObject]


class IRegistration:
    """Interface for registering with the central registry."""
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

    def retrieve(self, asset_id: Identifier, requester: Identifier
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


class IStepResult:
    """Contains and manages the outputs of a step.

    Some cleanup is needed after these have been stored, so they're
    wrapped up in this class so that we can add a utility function.

    Attributes:
        files: Dictionary mapping workflow items to Path objects, one
                for each output of the step. Each Path object points
                to an image file.
    """
    files: Dict[str, Path]

    def cleanup(self) -> None:
        """Cleans up associated resources.

        The asset image files will be gone after this has been called.
        """
        raise NotImplementedError()


class IDomainAdministrator:
    """Manages container resources for a site.

    The "domain" in the name is a system administration or networking
    domain, containing (virtual) networks and containers (and
    potentially virtual machines, (virtual) programmable networking
    hardware, etc. in which workflows are executed. Classes
    implementing this interface manage ("administrate") these resources
    in the domain to implement workflow execution.

    Network administration should be delegated to an
    INetworkAdministrator.
    """
    def execute_step(
            self, step: WorkflowStep, inputs: Dict[str, Asset],
            compute_asset: ComputeAsset, output_bases: Dict[str, Asset],
            id_hashes: Dict[str, str],
            step_subjob: Job) -> IStepResult:
        """Execute the given workflow step.

        Args:
            step: The step to execute.
            inputs: Input assets indexed by input name.
            compute_asset: The compute asset to run.
            output_bases: The base images to use for the outputs.
            id_hashes: A hash for each workflow item, indexed by its
                name.
            step_subjob: A subjob for the step's results' metadata.
        """
        raise NotImplementedError()

    def serve_asset(
            self, asset: Asset, connection_request: ConnectionRequest
            ) -> ConnectionInfo:
        """Serve an asset as a VPN-reachable service.

        Args:
            asset: The asset to serve.
            connection_request: A description of the remote end of the
                VPN connection to set up.

        Return:
            A connection info object describing how to connect to the
            local endpoint. The conn_id may be passed later to
            stop_serving_asset().
        """
        raise NotImplementedError()

    def stop_serving_asset(self, conn_id: str) -> None:
        """Stop serving an asset.

        Args:
            conn_id: A connection ID.
        """
        raise NotImplementedError()


class INetworkAdministrator:
    """Manages network resources for a site.

    The network administrator is in charge of the (virtual) networks
    and related things like routing and firewalling, as well as
    potentially (virtual) programmable networking hardware.

    Classes implementing this interface manage ("administrate") these
    resources to help implement workflow execution.
    """
    def serve_asset(
            self, conn_id: str, network_namespace: int,
            request: ConnectionRequest) -> ConnectionInfo:
        """Create a public endpoint to serve an asset.

        Args:
            conn_id: Connection id for this connection.
            network_namespace: PID of the network namespace to create
                    the endpoint inside of.
            request: Connection request from the client side.

        Return:
            A connection info object to send back to the client.
        """
        raise NotImplementedError()

    def stop_serving_asset(
            self, conn_id: str, network_namespace: int) -> None:
        """Remove a public endpoint and free resources.

        Args:
            conn_id: Id of the connection to stop.
            network_namespace: PID of the network namespace the
                    endpoint was created inside of.
        """
        raise NotImplementedError()

    def connect_to_inputs(
            self, job_id: int, inputs: Dict[str, Asset],
            network_namespace: int
            ) -> Tuple[Dict[str, str], Dict[str, Asset]]:
        """Connect a local network namespace to a set of inputs.

        Args:
            job_id: Job id for future reference.
            inputs: Assets to connect to, indexed by input name.
            network_namespace: Namespace to create network interfaces
                    inside of.

        Return:
            (nets, remaining) where nets contains a host IP for each
            successfully connected input, and remaining contains the
            assets from inputs that we could not connect to.
        """
        raise NotImplementedError()

    def disconnect_inputs(self, job_id: int, inputs: Dict[str, Asset]) -> None:
        """Disconnect inputs and free resources.

        Args:
            job_id: Job id of input connections to remove.
            inputs: The inputs for this job.
        """
        raise NotImplementedError()
