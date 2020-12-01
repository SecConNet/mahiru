from proof_of_concept.policy.rules import (
        InAssetCollection, InPartyCollection, MayAccess, ResultOfDataIn)


def test_in_asset_collection_signatures(private_key):
    rule = InAssetCollection(
            'asset:party1:dataset.asset1:party1:site1',
            'asset_collection:party1:collection.collection1')
    assert not rule.has_valid_signature(private_key.public_key())
    rule.sign(private_key)
    assert rule.has_valid_signature(private_key.public_key())
    rule.asset = 'asset:party1:dataset.asset2:party1:site1'
    assert not rule.has_valid_signature(private_key.public_key())
    rule.asset = 'asset:party1:dataset.asset1:party1:site1'
    assert rule.has_valid_signature(private_key.public_key())
    rule.collection = 'asset_collection:party2:collection.collection1'
    assert not rule.has_valid_signature(private_key.public_key())


def test_in_party_collection_signatures(private_key):
    rule = InPartyCollection(
            'party:ns1:party1',
            'party_collection:ns1:collection.parties')
    assert not rule.has_valid_signature(private_key.public_key())
    rule.sign(private_key)
    assert rule.has_valid_signature(private_key.public_key())
    rule.party = 'party:ns2:party2'
    assert not rule.has_valid_signature(private_key.public_key())
    rule.party = 'party:ns1:party1'
    assert rule.has_valid_signature(private_key.public_key())
    rule.collection = 'party_collection:ns2:collection.parties'
    assert not rule.has_valid_signature(private_key.public_key())


def test_may_access_signatures(private_key):
    rule = MayAccess('site:ns1:site1', 'asset:ns2:dataset.asset1:ns2:site2')
    assert not rule.has_valid_signature(private_key.public_key())
    rule.sign(private_key)
    assert rule.has_valid_signature(private_key.public_key())
    rule.site = 'site:ns1:site'
    assert not rule.has_valid_signature(private_key.public_key())
    rule.site = 'site:ns1:site1'
    assert rule.has_valid_signature(private_key.public_key())
    rule.asset = 'asset:ns1:dataset.asset2:ns1:site1'
    assert not rule.has_valid_signature(private_key.public_key())


def test_result_of_in_signatures(private_key):
    rule = ResultOfDataIn(
            'asset:ns1:dataset.asset1:ns1:site1',
            'asset:ns1:software.asset2:ns1:site1',
            'asset_collection:ns2:collection.collection1')

    assert not rule.has_valid_signature(private_key.public_key())
    rule.sign(private_key)
    assert rule.has_valid_signature(private_key.public_key())

    rule.data_asset = 'asset:ns2:dataset.test:ns2:site2'
    assert not rule.has_valid_signature(private_key.public_key())
    rule.data_asset = 'asset:ns1:dataset.asset1:ns1:site1'
    assert rule.has_valid_signature(private_key.public_key())

    rule.collection = 'asset_collection:ns2:collection.coll'
    assert not rule.has_valid_signature(private_key.public_key())
