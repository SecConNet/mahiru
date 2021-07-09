"""Definitions for replication of the registry."""
from mahiru.definitions.registry import RegisteredObject
from mahiru.replication import CanonicalStore, ReplicaUpdate


class RegistryUpdate(ReplicaUpdate[RegisteredObject]):
    """An update for registry replicas."""
    ReplicatedType = RegisteredObject


class RegistryStore(CanonicalStore[RegisteredObject]):
    """A canonical store for the registry."""
    UpdateType = RegistryUpdate
