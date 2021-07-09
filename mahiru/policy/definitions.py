"""Definitions related to policies."""
from mahiru.replication import ReplicaUpdate
from mahiru.definitions.policy import Rule


class PolicyUpdate(ReplicaUpdate[Rule]):
    """An update for policy replicas."""
    ReplicatedType = Rule
