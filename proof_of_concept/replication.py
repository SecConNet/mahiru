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


class Replicable:
    """Interface for objects that can be replicated.

    Derive your class from this and call super().__init__() in your
    __init__() method.
    """
    __time_created = None       # type: float
    __time_deleted = None       # type: Optional[float]

    def __init__(self) -> None:
        """Create a Replicable."""
        self.__time_created = time.time()
        self.__time_deleted = None          # type: Optional[float]

    def time_created(self) -> float:
        """Return the time when this record was created.

        This is expressed as a number of seconds since the Unix epoch.
        """
        return self.__time_created

    def time_deleted(self) -> Optional[float]:
        """Return the time when this record was deleted, if any.

        This is expressed as a number of seconds since the Unix epoch.
        If the object currently exists, then returns None. If the
        object no longer exists, returns a value larger than that
        returned by `time_created`_.
        """
        return self.__time_deleted

    def _delete(self) -> None:
        # friend: ReplicationServer
        self.__time_deleted = time.time()


T = TypeVar('T', bound=Replicable)


class ReplicableArchive(Generic[T]):
    """Stores an archive of replicable objects.

    This contains both existing and deleted objects. It models the raw
    database.
    """
    def __init__(self) -> None:
        """Create an empty archive."""
        self.objects = set()        # type: Set[T]
        self.timestamp = None       # type: Optional[float]


class CanonicalStore(Generic[T]):
    """Stores Replicables and can be replicated."""

    def __init__(self, archive: ReplicableArchive) -> None:
        """Create a ReplicatedStore."""
        self._archive = archive

    def objects(self) -> Iterable[T]:
        """Iterate through currently extant objects."""
        return [
                obj for obj in self._archive.objects
                if obj.time_deleted() is None]

    def insert(self, obj: T) -> None:
        """Insert an object into the collection of objects.

        Args:
            obj: A new object to add.
        """
        self._archive.objects.add(obj)

    def delete(self, obj: T) -> None:
        """Delete an object from the collection of objects.

        Args:
            obj: An object to delete.

        Raises:
            ValueError: If the object is not present.
        """
        if obj not in self._archive.objects:
            raise ValueError('Object not found')
        obj._delete()


class IReplicationServer(Generic[T]):
    """Generic interface for replication servers."""
    def get_updates_since(
            self, timestamp: Optional[float]
            ) -> Tuple[float, Set[T], Set[T]]:
        """Return a set of objects modified since the given time.

        Args:
            timestamp: A timestamp received from a previous call to
                    this function, or None to get all objects.

        Return:
            A new timestamp up to which this update updates the
                    receiver, and a set of newly created objects,
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
            self, timestamp: Optional[float]
            ) -> Tuple[float, Set[T], Set[T]]:
        """Return a set of objects modified since the given time.

        Args:
            timestamp: A timestamp received from a previous call to
                    this function, or None to get all objects.

        Return:
            A new timestamp up to which this update updates the
                    receiver, and a set of newly created objects,
                    and a set of newly deleted objects.
        """
        def deleted_after(
                timestamp: float, time_deleted: Optional[float]) -> bool:
            if time_deleted is None:
                return False
            return timestamp < time_deleted

        def deleted_before(
                time_deleted: Optional[float], timestamp: float) -> bool:
            if time_deleted is None:
                return False
            return time_deleted <= timestamp

        new_timestamp = time.time()
        if timestamp is None:
            new_objects = {
                    obj for obj in self._archive.objects
                    if obj.time_created() <= new_timestamp}
            deleted_objects = set()    # type: Set[T]
        else:
            new_objects = {
                    obj for obj in self._archive.objects
                    if (timestamp < obj.time_created()
                        and obj.time_created() <= new_timestamp)}

            deleted_objects = {
                    obj for obj in self._archive.objects
                    if (deleted_after(timestamp, obj.time_deleted())
                        and deleted_before(obj.time_deleted(), new_timestamp))}

        return new_timestamp, new_objects, deleted_objects


class Replica(Generic[T]):
    """Stores a replica of a CanonicalStore."""
    def __init__(self, server: IReplicationServer[T]) -> None:
        """Create an empty Replica."""
        self.objects = set()        # type: Set[T]

        self._timestamp = None       # type: Optional[float]
        self._server = server

    def lag(self) -> float:
        """Returns the number of seconds since the last update.

        Returns +infinity if no update was ever done.
        """
        if self._timestamp is None:
            return float('inf')
        return time.time() - self._timestamp

    def update(self) -> None:
        """Brings the replica up-to-date with the server."""
        new_time, new_objs, del_objs = self._server.get_updates_since(
                self._timestamp)
        # In a database, do this in a single transaction
        self.objects.difference_update(del_objs)
        self.objects.update(new_objs)
        self._timestamp = new_time
