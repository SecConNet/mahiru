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
from typing import Iterable, Optional, Set, Tuple


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


class ReplicatedStore:
    """Stores Replicables."""

    def __init__(self) -> None:
        """Create a ReplicatedStore for the given objects."""
        self._objects = set()      # type: Set[Replicable]

    def all_objects(self) -> Iterable[Replicable]:
        """Return all the stored objects, including deleted ones."""
        return self._objects

    def objects(self) -> Iterable[Replicable]:
        """Iterate through currently extant objects."""
        return [
                obj for obj in self._objects
                if obj.time_deleted() is None]

    def insert(self, obj: Replicable) -> None:
        """Insert an object into the collection of objects.

        Args:
            obj: A new object to add.
        """
        self._objects.add(obj)

    def delete(self, obj: Replicable) -> None:
        """Delete an object from the collection of objects.

        Args:
            obj: An object to delete.

        Raises:
            ValueError: If the object is not present.
        """
        if obj not in self._objects:
            raise ValueError('Object not found')
        obj._delete()


class ReplicationServer:
    """Serves Replicables from a set of them."""

    def __init__(self, store: ReplicatedStore) -> None:
        """Create a ReplicationServer for the given store.

        Args:
            store: A store to serve updates from.
        """
        self._store = store

    def get_updates_since(
            self, timestamp: Optional[float]
            ) -> Tuple[float, Set[Replicable], Set[Replicable]]:
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
                    obj for obj in self._store.all_objects()
                    if obj.time_created() <= new_timestamp}
            deleted_objects = set()    # type: Set[Replicable]
        else:
            new_objects = {
                    obj for obj in self._store.all_objects()
                    if (timestamp < obj.time_created()
                        and obj.time_created() <= new_timestamp)}

            deleted_objects = {
                    obj for obj in self._store.all_objects()
                    if (deleted_after(timestamp, obj.time_deleted())
                        and deleted_before(obj.time_deleted(), new_timestamp))}

        return new_timestamp, new_objects, deleted_objects


class ReplicationClient:
    """Maintains a replica of a set of Replicables.

    For all objects in `objects`, `time_deleted` will return None.

    Attributes:
        objects: A set of replicable objects.
    """

    def __init__(self, server: ReplicationServer) -> None:
        """Create a ReplicationClient for the given server.

        Args:
            server: The server to replicate from.
        """
        self.objects = set()       # type: Set[Replicable]

        self._server = server
        self._timestamp = None     # type: Optional[float]

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
