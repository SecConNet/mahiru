"""Definitions for replication of the registry."""
from proof_of_concept.definitions.registry import RegisteredObject
from proof_of_concept.replication import CanonicalStore, ReplicaUpdate


class RegistryUpdate(ReplicaUpdate[RegisteredObject]):
    """An update for registry replicas."""
    ReplicatedType = RegisteredObject


class RegistryStore(CanonicalStore[RegisteredObject]):
    """A canonical store for the registry."""
    UpdateType = RegistryUpdate
