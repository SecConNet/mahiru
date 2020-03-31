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
import time
from typing import Generic, Iterable, Optional, Set, Tuple, TypeVar


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
        """Create a ReplicatedStore."""
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


class IReplicationServer(Generic[T]):
    """Generic interface for replication servers."""
    def get_updates_since(
            self, from_version: Optional[int]
            ) -> Tuple[int, Set[T], Set[T]]:
        """Return a set of objects modified since the given version.

        Args:
            from_version: A version received from a previous call to
                    this function, or None to get an update for a
                    fresh replica.

        Return:
            A new version up to which this update updates the
                    receiver, a set of newly created objects,
                    and a set of newly deleted objects.
        """
        raise NotImplementedError()


class ReplicationServer(IReplicationServer[T]):
    """Serves Replicables from a set of them."""

    def __init__(self, archive: ReplicableArchive) -> None:
        """Create a ReplicationServer for the given archive.

        Args:
            archive: An archive to serve updates from.
        """
        self._archive = archive

    def get_updates_since(
            self, from_version: Optional[int]
            ) -> Tuple[int, Set[T], Set[T]]:
        """Return a set of objects modified since the given version.

        Args:
            from_version: A version received from a previous call to
                    this function, or None to get an update for a
                    fresh replica.

        Return:
            A new version up to which this update updates the
                    receiver, a set of newly created objects,
                    and a set of newly deleted objects.
        """
        def deleted_after(version: int, deleted: Optional[int]) -> bool:
            if deleted is None:
                return True
            return version < deleted

        def deleted_before(deleted: Optional[int], version: int) -> bool:
            if deleted is None:
                return False
            return deleted <= version

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

        return to_version, new_objects, deleted_objects


class Replica(Generic[T]):
    """Stores a replica of a CanonicalStore."""
    def __init__(self, server: IReplicationServer[T]) -> None:
        """Create an empty Replica."""
        self.objects = set()        # type: Set[T]

        self._server = server
        self._version = None        # type: Optional[int]
        self._timestamp = None      # type: Optional[float]

    def lag(self) -> float:
        """Returns the number of seconds since the last update.

        Returns +infinity if no update was ever done.
        """
        if self._timestamp is None:
            return float('inf')
        return time.time() - self._timestamp

    def update(self) -> None:
        """Brings the replica up-to-date with the server."""
        new_version, new_objs, del_objs = self._server.get_updates_since(
                self._version)
        # In a database, do this in a single transaction
        self.objects.difference_update(del_objs)
        self.objects.update(new_objs)
        self._version = new_version
        self._timestamp = time.time()
