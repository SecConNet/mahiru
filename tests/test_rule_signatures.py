from proof_of_concept.policy import (
        InAssetCollection, InPartyCollection, MayAccess, ResultOfDataIn)


def test_in_asset_collection_signatures(private_key):
    rule = InAssetCollection(
            'id:party1/dataset/asset1', 'id:party1/collection/collection1')
    assert not rule.has_valid_signature(private_key.public_key())
    rule.sign(private_key)
    assert rule.has_valid_signature(private_key.public_key())
    rule.asset = 'id:party1/dataset/asset2'
    assert not rule.has_valid_signature(private_key.public_key())
    rule.asset = 'id:party1/dataset/asset1'
    assert rule.has_valid_signature(private_key.public_key())
    rule.collection = 'id:party2/collection/collection1'
    assert not rule.has_valid_signature(private_key.public_key())


def test_in_party_collection_signatures(private_key):
    rule = InPartyCollection(
            'id:party1', 'id:party1/collection/parties')
    assert not rule.has_valid_signature(private_key.public_key())
    rule.sign(private_key)
    assert rule.has_valid_signature(private_key.public_key())
    rule.party = 'id:party2'
    assert not rule.has_valid_signature(private_key.public_key())
    rule.party = 'id:party1'
    assert rule.has_valid_signature(private_key.public_key())
    rule.collection = 'id:party2/collection/parties'
    assert not rule.has_valid_signature(private_key.public_key())


def test_may_access_signatures(private_key):
    rule = MayAccess('id:site1', 'id:party2/dataset/asset1')
    assert not rule.has_valid_signature(private_key.public_key())
    rule.sign(private_key)
    assert rule.has_valid_signature(private_key.public_key())
    rule.site = 'id:site'
    assert not rule.has_valid_signature(private_key.public_key())
    rule.site = 'id:site1'
    assert rule.has_valid_signature(private_key.public_key())
    rule.asset = 'id:party1/dataset/asset2'
    assert not rule.has_valid_signature(private_key.public_key())


def test_result_of_in_signatures(private_key):
    rule = ResultOfDataIn(
            'id:party1/dataset/asset1', 'id:party1/software/asset2',
            'id:party2/collection/collection1')

    assert not rule.has_valid_signature(private_key.public_key())
    rule.sign(private_key)
    assert rule.has_valid_signature(private_key.public_key())

    rule.data_asset = 'id:party2/dataset/test'
    assert not rule.has_valid_signature(private_key.public_key())
    rule.data_asset = 'id:party1/dataset/asset1'
    assert rule.has_valid_signature(private_key.public_key())

    rule.compute_asset = 'party1/software/asset2'
    assert not rule.has_valid_signature(private_key.public_key())
    rule.compute_asset = 'id:party1/software/asset2'
    assert rule.has_valid_signature(private_key.public_key())

    rule.collection = 'id:party2/collection/coll'
    assert not rule.has_valid_signature(private_key.public_key())
