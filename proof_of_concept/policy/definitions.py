"""Definitions related to policies."""
from proof_of_concept.replication import ReplicaUpdate
from proof_of_concept.definitions.policy import Rule


class PolicyUpdate(ReplicaUpdate[Rule]):
    """An update for policy replicas."""
    ReplicatedType = Rule
