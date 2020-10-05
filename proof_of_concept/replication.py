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
import requests
import time
from typing import (
        Any, Dict, Generic, Iterable, Optional, Set, Tuple, Type, TypeVar)

from falcon import Request, Response

from proof_of_concept.definitions import IReplicationSource, ReplicaUpdate


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


class CanonicalStore(Generic[T]):
    """Stores Replicables and can be replicated."""

    def __init__(self, archive: ReplicableArchive) -> None:
        """Create a CanonicalStore."""
        self._archive = archive

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


class ObjectValidator(Generic[T]):
    """Validates incoming replica updates."""
    def is_valid(self, received_object: T) -> bool:
        """Returns True iff the object is valid."""
        raise NotImplementedError()


class ReplicationServer(IReplicationSource[T]):
    """Serves Replicables from a set of them."""

    def __init__(self, archive: ReplicableArchive, max_lag: float) -> None:
        """Create a ReplicationServer for the given archive.

        Args:
            archive: An archive to serve updates from.
            max_lag: Maximum time (s) replicas may be out of date.
        """
        self._archive = archive
        self._max_lag = max_lag

    def get_updates_since(
            self, from_version: Optional[int]
            ) -> ReplicaUpdate[T]:
        """Return a set of objects modified since the given version.

        Args:
            from_version: A version received from a previous call to
                    this function, or None to get an update for a
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

        cur_time = time.time()
        to_version = self._archive.version
        if from_version is None:
            from_version = -1

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

        valid_until = cur_time + self._max_lag
        return ReplicaUpdate(
                from_version, to_version, valid_until,
                new_objects, deleted_objects)


class Replica(Generic[T]):
    """Stores a replica of a CanonicalStore."""
    def __init__(
            self, source: IReplicationSource[T],
            validator: Optional[ObjectValidator[T]] = None
            ) -> None:
        """Create an empty Replica."""
        self.objects = set()        # type: Set[T]

        self._source = source
        self._validator = validator
        self._version = None        # type: Optional[int]
        self._valid_until = 0.0     # type: float

    def is_valid(self) -> bool:
        """Whether the replica is valid or outdated.

        Return:
            True iff the replica is now up-to-date enough according to
            the server.
        """
        return time.time() < self._valid_until

    def update(self) -> None:
        """Updates the replica, if necessary."""
        if not self.is_valid():
            update = self._source.get_updates_since(self._version)
            if self._validator is not None:
                for r in update.created:
                    if not self._validator.is_valid(r):
                        return
                for r in update.deleted:
                    if not self._validator.is_valid(r):
                        return

            # In a database, do this in a single transaction
            self.objects.difference_update(update.deleted)
            self.objects.update(update.created)
            self._version = update.to_version
            self._valid_until = update.valid_until
