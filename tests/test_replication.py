from proof_of_concept.replication import (
        Replicable, ReplicationServer, ReplicationClient)


class A(Replicable):
    def __init__(self) -> None:
        super().__init__()

    def __repr__(self) -> str:
        return 'A({}, {})'.format(self.time_created(), self.time_deleted())


def test_replication():
    a1 = A()
    a2 = A()

    server = ReplicationServer()
    server.insert(a1)
    server.insert(a2)

    client = ReplicationClient(server)

    assert client.objects == set()
    assert client.lag() == float('inf')
    client.update()
    assert client.objects == {a1, a2}
    assert client.lag() > 0.0
    assert client.lag() < 1.0

    a3 = A()
    server.insert(a3)
    assert client.objects == {a1, a2}
    client.update()
    assert client.objects == {a1, a2, a3}

    server.delete(a2)
    assert client.objects == {a1, a2, a3}
    client.update()
    assert set(client.objects) == {a1, a3}

# This could do with some unit testing of server and client
