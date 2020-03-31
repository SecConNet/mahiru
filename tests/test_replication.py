from proof_of_concept.replication import (
        CanonicalStore, Replica, Replicable, ReplicableArchive,
        ReplicationServer)


class A:
    pass


def test_replication():
    archive = ReplicableArchive()
    store = CanonicalStore(archive)
    server = ReplicationServer(archive)
    replica = Replica(server)

    a1 = A()
    store.insert(a1)
    a2 = A()
    store.insert(a2)

    assert store.objects() == {a1, a2}
    a1_record = [r for r in archive.records if r.object == a1][0]
    assert a1_record.created == 1
    assert a1_record.deleted is None
    a2_record = [r for r in archive.records if r.object == a2][0]
    assert a2_record.created == 2
    assert a2_record.deleted is None

    assert replica.objects == set()
    assert replica.lag() == float('inf')
    replica.update()
    assert replica.objects == {a1, a2}
    assert replica.lag() > 0.0
    assert replica.lag() < 1.0

    a3 = A()
    store.insert(a3)
    assert replica.objects == {a1, a2}
    replica.update()
    assert replica.objects == {a1, a2, a3}

    store.delete(a2)
    assert a2_record.created == 2
    assert a2_record.deleted == 4

    assert replica.objects == {a1, a2, a3}
    replica.update()
    assert set(replica.objects) == {a1, a3}

# This could do with some unit testing of store, server and replica
