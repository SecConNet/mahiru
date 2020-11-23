"""Widely used interface definitions."""
from datetime import datetime
from typing import Generic, Iterable, Set, Type, TypeVar

from proof_of_concept.definitions.assets import Asset
from proof_of_concept.definitions.policy import Rule
from proof_of_concept.definitions.workflows import JobSubmission


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


class IPolicyCollection:
    """Provides policies to a PolicyEvaluator."""
    def policies(self) -> Iterable[Rule]:
        """Returns an iterable collection of rules."""
        raise NotImplementedError()


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
