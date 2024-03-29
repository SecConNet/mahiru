"""A simple data replication system.

Note: an implementation backed by a database needs to ensure strict
serialisability, or use some kind of consistent counter for the time
stamps. If an object with a time_created before the last replication is
added, then it won't be replicated in the next replication and will
thus be missing from the replica. Likewise for deletion in the past.

Considering that we'll only replicate low-velocity data such as
policies and site and asset metadata, just enabling strict
serialisation is probably the way to go. That's what we do here, using
the Python GIL.
"""
from datetime import datetime, timedelta
import logging
from typing import (
        Callable, Generic, Iterable, Optional, Set, Type,
        TypeVar)

from mahiru.definitions.interfaces import IReplicaUpdate, IReplicationService


logger = logging.getLogger(__name__)


T = TypeVar('T')


class Replicable(Generic[T]):
    """Wrapper for objects that are to be replicated.

    Attributes:
        created: The first version from which this object exists.
        deleted: The first version from which this object no
                longer exists.
        object: The wrapped object.
    """
    def __init__(self, created: int, obj: T) -> None:
        """Create a Replicable wrapping an object.

        Args:
            created: The version in which this object first existed.
            obj: The object to be wrapped.
        """
        self.created = created
        self.deleted = None     # type: Optional[int]
        self.object = obj

    def __repr__(self) -> str:
        """Returns a string representation of the object."""
        return 'Replicable({}, {}, {})'.format(
                self.created, self.deleted, self.object)


class ReplicableArchive(Generic[T]):
    """Stores an archive of replicable objects.

    This contains both existing and deleted objects. It models the raw
    database.

    Attributes:
        records: The stored records, encoding all versions of the data
                set.
        version: The current (latest) version of the data.
    """
    def __init__(self) -> None:
        """Create an empty archive."""
        self.records = set()        # type: Set[Replicable[T]]
        self.version = 0            # type: int


class ReplicaUpdate(IReplicaUpdate[T]):
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


class CanonicalStore(IReplicationService[T]):
    """Stores Replicables and can be replicated."""
    UpdateType = ReplicaUpdate[T]   # type: Type[ReplicaUpdate[T]]

    def __init__(self, archive: ReplicableArchive, max_lag: float) -> None:
        """Create a CanonicalStore.

        Args:
            archive: The archive to use to store objects.
            max_lag: Maximum time (s) replicas may be out of date.
        """
        self._archive = archive
        self._max_lag = max_lag

    def objects(self) -> Iterable[T]:
        """Iterate through currently extant objects."""
        return {
                rec.object for rec in self._archive.records
                if rec.deleted is None}

    def insert(self, obj: T) -> None:
        """Insert an object into the collection of objects.

        Args:
            obj: A new object to add.
        """
        new_version = self._archive.version + 1
        self._archive.records.add(Replicable(new_version, obj))
        self._archive.version = new_version

    def delete(self, obj: T) -> None:
        """Delete an object from the collection of objects.

        Args:
            obj: An object to delete.

        Raises:
            ValueError: If the object is not present.
        """
        new_version = self._archive.version + 1
        for rec in self._archive.records:
            if rec.object == obj:
                rec.deleted = new_version
                break
        else:
            raise ValueError('Object not found')
        self._archive.version = new_version

    def get_updates_since(self, from_version: int) -> ReplicaUpdate[T]:
        """Return a set of objects modified since the given version.

        Args:
            from_version: A version received from a previous call to
                    this function, or 0 to get an update for a
                    fresh replica.

        Return:
            An update from the given version to a newer version.
        """
        def deleted_after(version: int, deleted: Optional[int]) -> bool:
            if deleted is None:
                return True
            return version < deleted

        def deleted_before(deleted: Optional[int], version: int) -> bool:
            if deleted is None:
                return False
            return deleted <= version

        cur_time = datetime.now()
        to_version = self._archive.version

        new_objects = {
                rec.object for rec in self._archive.records
                if (from_version < rec.created and
                    rec.created <= to_version and
                    deleted_after(to_version, rec.deleted))}

        deleted_objects = {
                rec.object for rec in self._archive.records
                if (rec.created <= from_version and
                    deleted_after(from_version, rec.deleted) and
                    deleted_before(rec.deleted, to_version))}

        readded_objects = new_objects.intersection(deleted_objects)
        new_objects -= readded_objects
        deleted_objects -= readded_objects

        valid_until = cur_time + timedelta(seconds=self._max_lag)
        return self.UpdateType(
                from_version, to_version, valid_until,
                new_objects, deleted_objects)


class ObjectValidator(Generic[T]):
    """Validates incoming replica updates."""
    def is_valid(self, received_object: T) -> bool:
        """Returns True iff the object is valid."""
        raise NotImplementedError()


class Replica(Generic[T]):
    """Stores a replica of a CanonicalStore."""
    def __init__(
            self, source: IReplicationService[T],
            validator: Optional[ObjectValidator[T]] = None,
            on_update: Optional[Callable[[Set[T], Set[T]], None]] = None
            ) -> None:
        """Create an empty Replica.

        The callback function, if specified, will be called by update()
        if there are any changes to the replica. It must be a callable
        object taking a set of newly created T as its first argument,
        and a set of newly deleted T as its second argument.

        Args:
            source: Source to get replica updates from.
            validator: Validates incoming objects, if specified.
            on_update: Called with changes when update() is called.
        """
        self.objects = set()        # type: Set[T]

        self._source = source
        self._validator = validator
        self._on_update = on_update

        self._version = 0
        self._valid_until = datetime.fromtimestamp(0.0)

    def is_valid(self) -> bool:
        """Whether the replica is valid or outdated.

        Return:
            True iff the replica is now up-to-date enough according to
            the server.
        """
        return datetime.now() < self._valid_until

    def update(self) -> None:
        """Updates the replica, if necessary."""
        if not self.is_valid():
            update = self._source.get_updates_since(self._version)
            if self._validator is not None:
                for r in update.created:
                    if not self._validator.is_valid(r):
                        logger.error(f'Object {r} failed validation.')
                        return
                for r in update.deleted:
                    if not self._validator.is_valid(r):
                        logger.error(f'Object {r} failed validation.')
                        return

            # In a database, do this in a single transaction
            self.objects.difference_update(update.deleted)
            self.objects.update(update.created)
            self._version = update.to_version
            self._valid_until = update.valid_until

            if self._on_update:
                self._on_update(update.created, update.deleted)
