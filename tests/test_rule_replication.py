from copy import copy

from mahiru.policy.replication import PolicyStore
from mahiru.policy.rules import (
        InAssetCollection, InPartyCollection, MayAccess, ResultOfDataIn,
        ResultOfComputeIn)
from mahiru.replication import ReplicableArchive
from mahiru.definitions.policy import Rule


def test_rules_are_values():
    # If a rule in a policy store is removed and then reinserted, the
    # reinserted version of the rule should be considered the same
    # rule, and if a replica update is requested from a version in
    # which the rule was present to a version in which it is again
    # present, then the rule should not show up in the update at all.

    # This test runs a small scenario in which a rule is inserted and
    # deleted and reinserted amidst some other changes, and checks that
    # it is absent from an update straddling the deletion and
    # reinsertion.
    archive = ReplicableArchive[Rule]()
    policy_store = PolicyStore(archive, 1.0)

    rule1 = InAssetCollection(
            'asset:party1_ns:data1:ns:s',
            'asset_collection:party1_ns:collection1')
    rule2 = InPartyCollection(
            'party:party2_ns:party2', 'party_collection:party2_ns:collection2')
    rule3 = MayAccess('site:party3_ns:site3', 'asset:party3_ns:data3:ns:s')

    for rule in (rule1, rule2, rule3):
        policy_store.insert(copy(rule))

    update1 = policy_store.get_updates_since(0)
    assert update1.created == {rule1, rule2, rule3}

    for rule in (rule1, rule2):
        policy_store.delete(copy(rule))

    policy_store.insert(copy(rule1))

    rule4a = ResultOfDataIn(
            'asset:party4_ns:data4:ns:s', 'asset:party4_ns:compute4:ns:s',
            'asset_collection:party4_ns:collection4')
    rule4b = ResultOfComputeIn(
            'asset:party4_ns:data4:ns:s', 'asset:party4_ns:compute4:ns:s',
            'asset_collection:party4_ns:collection4')
    policy_store.insert(copy(rule4a))
    policy_store.insert(copy(rule4b))

    update2 = policy_store.get_updates_since(update1.to_version)
    assert update2.deleted == {rule2}
    assert update2.created == {rule4a, rule4b}
