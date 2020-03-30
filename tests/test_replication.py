from proof_of_concept.replication import (
        Replicable, ReplicatedStore, ReplicationServer, ReplicationClient)


class A(Replicable):
    def __init__(self) -> None:
        super().__init__()

    def __repr__(self) -> str:
        return 'A({}, {})'.format(self.time_created(), self.time_deleted())


def test_replication():
    store = ReplicatedStore()
    server = ReplicationServer(store)
    client = ReplicationClient(server)

    a1 = A()
    store.insert(a1)
    a2 = A()
    store.insert(a2)

    assert store.archive() == {a1, a2}
    assert client.objects == set()
    assert client.lag() == float('inf')
    client.update()
    assert client.objects == {a1, a2}
    assert client.lag() > 0.0
    assert client.lag() < 1.0

    a3 = A()
    store.insert(a3)
    assert client.objects == {a1, a2}
    client.update()
    assert client.objects == {a1, a2, a3}

    store.delete(a2)
    assert client.objects == {a1, a2, a3}
    client.update()
    assert set(client.objects) == {a1, a3}

# This could do with some unit testing of store, server and client
