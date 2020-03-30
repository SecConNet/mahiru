from proof_of_concept.replication import (
        CanonicalStore, Replica, Replicable, ReplicableArchive,
        ReplicationServer)


class A(Replicable):
    def __init__(self) -> None:
        super().__init__()

    def __repr__(self) -> str:
        return 'A({}, {})'.format(self.time_created(), self.time_deleted())


def test_replication():
    archive = ReplicableArchive()
    store = CanonicalStore(archive)
    server = ReplicationServer(archive)
    replica = Replica(server)

    a1 = A()
    store.insert(a1)
    a2 = A()
    store.insert(a2)

    assert archive.objects == {a1, a2}
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
    assert replica.objects == {a1, a2, a3}
    replica.update()
    assert set(replica.objects) == {a1, a3}

# This could do with some unit testing of store, server and replica
