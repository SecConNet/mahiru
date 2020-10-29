from datetime import datetime, timedelta
from unittest.mock import MagicMock
import time

from proof_of_concept.replication import (
        CanonicalStore, Replica, Replicable, ReplicableArchive,
        ReplicationServer, ReplicaUpdate)


class A:
    def __init__(self, name):
        self.name = name


def test_replication():
    REPLICA_LAG = 0.01

    archive = ReplicableArchive()
    store = CanonicalStore(archive)
    server = ReplicationServer(archive, REPLICA_LAG)
    replica = Replica(server)

    a1 = A('a1')
    store.insert(a1)
    a2 = A('a2')
    store.insert(a2)

    assert store.objects() == {a1, a2}
    a1_record = [r for r in archive.records if r.object == a1][0]
    assert a1_record.created == 1
    assert a1_record.deleted is None
    a2_record = [r for r in archive.records if r.object == a2][0]
    assert a2_record.created == 2
    assert a2_record.deleted is None

    assert replica.objects == set()
    replica.update()
    assert replica.objects == {a1, a2}

    a3 = A('a3')
    store.insert(a3)
    assert replica.objects == {a1, a2}
    replica.update()
    assert replica.objects == {a1, a2}
    time.sleep(REPLICA_LAG)
    replica.update()
    assert replica.objects == {a1, a2, a3}

    store.delete(a2)
    assert a2_record.created == 2
    assert a2_record.deleted == 4

    assert replica.objects == {a1, a2, a3}
    time.sleep(REPLICA_LAG)
    replica.update()
    assert set(replica.objects) == {a1, a3}


class Validator:
    def is_valid(self, x):
        return x.name[0] == 'a'


def test_validation():
    a1 = A('a1')
    a2 = A('a2')
    a3 = A('a3')
    b1 = A('b1')

    server = MagicMock()
    server.get_updates_since.return_value = ReplicaUpdate(
            0, 2, datetime.now() + timedelta(seconds=0.01), {a1, a2}, {})
    replica = Replica(server, Validator())
    assert not replica.is_valid()
    replica.update()
    assert replica.is_valid()
    assert replica.objects == {a1, a2}

    time.sleep(0.01)
    assert not replica.is_valid()
    server.get_updates_since.return_value = ReplicaUpdate(
            2, 3, time.time() + 1.0, {b1}, {})
    replica.update()
    assert not replica.is_valid()


# This could do with some unit testing of store, server and replica
