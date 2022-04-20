from typing import Iterable, List

import pytest

from mahiru.definitions.identifier import Identifier
from mahiru.definitions.interfaces import IPolicyCollection
from mahiru.policy.evaluation import Permissions, PolicyEvaluator
from mahiru.policy.rules import (
        InAssetCategory, InAssetCollection, InSiteCategory, MayAccess,
        ResultOfComputeIn, ResultOfDataIn, Rule)


@pytest.fixture
def any_object() -> Identifier:
    """Return a wildcard identifier."""
    return Identifier('*')


@pytest.fixture
def asset1() -> Identifier:
    """Identifier for an asset owned by party1."""
    return Identifier('asset:ns1:asset1:ns1:site1')


@pytest.fixture
def asset2() -> Identifier:
    """Identifier for an asset owned by party2."""
    return Identifier('asset:ns2:asset2:ns2:site2')


@pytest.fixture
def asset3() -> Identifier:
    """Identifier for an asset owned by party3."""
    return Identifier('asset:ns3:asset3:ns3:site3')


@pytest.fixture
def site1() -> Identifier:
    """Identifier for a site owned by party1."""
    return Identifier('site:ns1:site1')


@pytest.fixture
def site2() -> Identifier:
    """Identifier for a site owned by party2."""
    return Identifier('site:ns2:site2')


@pytest.fixture
def site_category1a() -> Identifier:
    """Identifier for a site category owned by party1."""
    return Identifier('site_category:ns1:site_category_a')


@pytest.fixture
def asset_collection1a() -> Identifier:
    """Identifier for an asset collection owned by party1."""
    return Identifier('asset_collection:ns1:asset_collection_a')


@pytest.fixture
def asset_collection1b() -> Identifier:
    """Identifier for an asset collection owned by party1."""
    return Identifier('asset_collection:ns1:asset_collection_b')


@pytest.fixture
def asset_collection1c() -> Identifier:
    """Identifier for an asset collection owned by party1."""
    return Identifier('asset_collection:ns1:asset_collection_c')


@pytest.fixture
def asset_collection2a() -> Identifier:
    """Identifier for an asset collection owned by party2."""
    return Identifier('asset_collection:ns2:asset_collection_a')


@pytest.fixture
def asset_collection2b() -> Identifier:
    """Identifier for an asset collection owned by party2."""
    return Identifier('asset_collection:ns2:asset_collection_b')


@pytest.fixture
def asset_collection3a() -> Identifier:
    """Identifier for an asset collection owned by party3."""
    return Identifier('asset_collection:ns3:asset_collection_a')


@pytest.fixture
def asset_category1a() -> Identifier:
    """Identifier for an asset category owned by party1."""
    return Identifier('asset_category:ns1:asset_category_a')


@pytest.fixture
def asset_category1b() -> Identifier:
    """Identifier for an asset category owned by party1."""
    return Identifier('asset_category:ns1:asset_category_b')


@pytest.fixture
def asset_category2a() -> Identifier:
    """Identifier for an asset category owned by party2."""
    return Identifier('asset_category:ns2:asset_category_a')


@pytest.fixture
def asset_category2b() -> Identifier:
    """Identifier for an asset category owned by party2."""
    return Identifier('asset_category:ns2:asset_category_b')


@pytest.fixture
def asset_category3a() -> Identifier:
    """Identifier for an asset category owned by party3."""
    return Identifier('asset_category:ns3:asset_category_a')


class MockPolicies(IPolicyCollection):
    def __init__(self, policies: List[Rule]) -> None:
        self._policies = policies

    def policies(self) -> Iterable[Rule]:
        return self._policies


# MayAccess tests


def test_primary_asset_access(asset1, site1, site2):
    policies = MockPolicies([MayAccess(site1, asset1)])
    evaluator = PolicyEvaluator(policies)

    perms = evaluator.permissions_for_asset(asset1)
    assert perms._sets == [{asset1}]
    assert evaluator.may_access(perms, site1)
    assert not evaluator.may_access(perms, site2)


def test_asset_collection_access(asset1, asset_collection1a, site1, site2):
    policies = MockPolicies([
        InAssetCollection(asset1, asset_collection1a),
        MayAccess(site1, asset_collection1a)])
    evaluator = PolicyEvaluator(policies)

    perms = evaluator.permissions_for_asset(asset1)
    assert perms._sets == [{asset1, asset_collection1a}]
    assert evaluator.may_access(perms, site1)
    assert not evaluator.may_access(perms, site2)


def test_asset_category_access(asset1, asset_category1a, site1):
    policies = MockPolicies([
        InAssetCategory(asset1, asset_category1a),
        MayAccess(site1, asset_category1a)])
    evaluator = PolicyEvaluator(policies)

    perms = evaluator.permissions_for_asset(asset1)
    assert perms._sets == [{asset1}]
    assert not evaluator.may_access(perms, site1)


def test_site_category_access(asset1, site_category1a, site2):
    policies = MockPolicies([
        InSiteCategory(site2, site_category1a),
        MayAccess(site_category1a, asset1)])
    evaluator = PolicyEvaluator(policies)

    perms = evaluator.permissions_for_asset(asset1)
    assert perms._sets == [{asset1}]
    assert evaluator.may_access(perms, site2)


def test_asset_category_any_site(any_object, asset1, asset_category1a, site1):
    policies = MockPolicies([
        InAssetCategory(asset1, asset_category1a),
        MayAccess(any_object, asset_category1a)])
    evaluator = PolicyEvaluator(policies)

    perms = evaluator.permissions_for_asset(asset1)
    assert perms._sets == [{asset1}]
    assert not evaluator.may_access(perms, site1)


# Propagation tests


def test_propagate_result_of_data_in(asset1, asset2, asset_collection1a):
    policies = MockPolicies([
        ResultOfDataIn(asset1, asset2, 'output1', asset_collection1a)])
    evaluator = PolicyEvaluator(policies)

    input_perms = [Permissions([{asset1}])]
    perms = evaluator.propagate_permissions(input_perms, asset2, 'output1')
    assert perms._sets == [{asset_collection1a}, set()]


def test_propagate_result_of_data_in_data_collection(
        asset1, asset2, asset_collection1a, asset_collection1b):
    policies = MockPolicies([
        InAssetCollection(asset1, asset_collection1a),
        ResultOfDataIn(
            asset_collection1a, asset2, 'output1', asset_collection1b)])
    evaluator = PolicyEvaluator(policies)

    input_perms = [Permissions([{asset1}])]
    perms = evaluator.propagate_permissions(input_perms, asset2, 'output1')
    assert perms._sets == [{asset_collection1b}, set()]


def test_propagate_result_of_data_in_data_category(
        asset1, asset2, asset_category1a, asset_collection1a):
    policies = MockPolicies([
        InAssetCategory(asset1, asset_category1a),
        ResultOfDataIn(
            asset_category1a, asset2, 'output1', asset_collection1a)])
    evaluator = PolicyEvaluator(policies)

    input_perms = [Permissions([{asset1}])]
    perms = evaluator.propagate_permissions(input_perms, asset2, 'output1')
    assert perms._sets == [set(), set()]


def test_propagate_result_of_data_in_compute_collection(
        asset1, asset2, asset_collection1a, asset_collection1b):
    policies = MockPolicies([
        InAssetCollection(asset1, asset_collection1a),
        ResultOfDataIn(
            asset2, asset_collection1a, 'output1', asset_collection1b)])
    evaluator = PolicyEvaluator(policies)

    input_perms = [Permissions([{asset2}])]
    perms = evaluator.propagate_permissions(input_perms, asset2, 'output1')
    assert perms._sets == [set(), set()]


def test_propagate_result_of_data_in_compute_category(
        asset1, asset2, asset_collection1a, asset_category1a):
    policies = MockPolicies([
        InAssetCategory(asset1, asset_category1a),
        ResultOfDataIn(
            asset2, asset_category1a, 'output1', asset_collection1a)])
    evaluator = PolicyEvaluator(policies)

    input_perms = [Permissions([{asset2}])]
    perms = evaluator.propagate_permissions(input_perms, asset1, 'output1')
    assert perms._sets == [{asset_collection1a}, set()]


def test_propagate_result_of_compute_in(asset1, asset2, asset_collection2a):
    policies = MockPolicies([
        ResultOfComputeIn(asset1, asset2, 'output1', asset_collection2a)])
    evaluator = PolicyEvaluator(policies)

    input_perms = [Permissions([{asset1}])]
    perms = evaluator.propagate_permissions(input_perms, asset2, 'output1')
    assert perms._sets == [set(), {asset_collection2a}]


def test_propagate_result_of_compute_in_data_collection(
        asset1, asset2, asset_collection1a, asset_collection2a):
    policies = MockPolicies([
        InAssetCollection(asset1, asset_collection1a),
        ResultOfComputeIn(
            asset_collection1a, asset2, 'output1', asset_collection2a)])
    evaluator = PolicyEvaluator(policies)

    input_perms = [Permissions([{asset1}])]
    perms = evaluator.propagate_permissions(input_perms, asset2, 'output1')
    assert perms._sets == [set(), set()]


def test_propagate_result_of_compute_in_data_category(
        asset1, asset2, asset_category2a, asset_collection2a):
    policies = MockPolicies([
        InAssetCategory(asset1, asset_category2a),
        ResultOfComputeIn(
            asset_category2a, asset2, 'output1', asset_collection2a)])
    evaluator = PolicyEvaluator(policies)

    input_perms = [Permissions([{asset1}])]
    perms = evaluator.propagate_permissions(input_perms, asset2, 'output1')
    assert perms._sets == [set(), {asset_collection2a}]


def test_propagate_result_of_compute_in_compute_collection(
        asset1, asset2, asset_collection1a, asset_collection1b):
    policies = MockPolicies([
        InAssetCollection(asset1, asset_collection1a),
        ResultOfComputeIn(
            asset2, asset_collection1a, 'output1', asset_collection1b)])
    evaluator = PolicyEvaluator(policies)

    input_perms = [Permissions([{asset2}])]
    perms = evaluator.propagate_permissions(input_perms, asset1, 'output1')
    assert perms._sets == [set(), {asset_collection1b}]


def test_propagate_result_of_compute_in_compute_category(
        asset1, asset2, asset_collection1a, asset_category1a):
    policies = MockPolicies([
        InAssetCategory(asset1, asset_category1a),
        ResultOfComputeIn(
            asset2, asset_category1a, 'output1', asset_collection1a)])
    evaluator = PolicyEvaluator(policies)

    input_perms = [Permissions([{asset2}])]
    perms = evaluator.propagate_permissions(input_perms, asset1, 'output1')
    assert perms._sets == [set(), set()]


def test_propagate_result_of_data_in_deep1(
        asset1, asset_collection1a, asset_collection1b,
        asset2, asset_category1a, asset_category1b,
        asset_collection1c):

    policies = MockPolicies([
        InAssetCollection(asset1, asset_collection1a),
        InAssetCollection(asset_collection1a, asset_collection1b),
        InAssetCategory(asset2, asset_category1a),
        InAssetCategory(asset_category1a, asset_category1b),
        ResultOfDataIn(
            asset_collection1b, asset_category1b, 'output1',
            asset_collection1c)])
    evaluator = PolicyEvaluator(policies)

    input_perms = [Permissions([{asset1}])]
    perms = evaluator.propagate_permissions(input_perms, asset2, 'output1')
    assert perms._sets == [{asset_collection1c}, set()]


def test_propagate_result_of_data_in_deep2(
        asset1, asset_collection1a, asset_collection1b,
        asset2, asset_collection1c):

    policies = MockPolicies([
        InAssetCollection(asset1, asset_collection1a),
        InAssetCollection(asset_collection1b, asset_collection1a),
        ResultOfDataIn(
            asset_collection1b, asset2, 'output1', asset_collection1c)])
    evaluator = PolicyEvaluator(policies)

    input_perms = [Permissions([{asset1}])]
    perms = evaluator.propagate_permissions(input_perms, asset2, 'output1')
    assert perms._sets == [set(), set()]


def test_propagate_result_of_data_in_deep3(
        asset1, asset2, asset_category1a, asset_category1b,
        asset_collection1c):

    policies = MockPolicies([
        InAssetCategory(asset2, asset_category1a),
        InAssetCategory(asset_category1b, asset_category1a),
        ResultOfDataIn(
            asset1, asset_category1b, 'output1', asset_collection1c)])
    evaluator = PolicyEvaluator(policies)

    input_perms = [Permissions([{asset1}])]
    perms = evaluator.propagate_permissions(input_perms, asset2, 'output1')
    assert perms._sets == [set(), set()]


def test_propagate_result_of_compute_in_deep1(
        asset1, asset_collection1a, asset_collection1b,
        asset2, asset_category1a, asset_category1b,
        asset_collection1c):

    policies = MockPolicies([
        InAssetCollection(asset1, asset_collection1a),
        InAssetCollection(asset_collection1a, asset_collection1b),
        InAssetCategory(asset2, asset_category1a),
        InAssetCategory(asset_category1a, asset_category1b),
        ResultOfComputeIn(
            asset_category1b, asset_collection1b, 'output1',
            asset_collection1c)])
    evaluator = PolicyEvaluator(policies)

    input_perms = [Permissions([{asset2}])]
    perms = evaluator.propagate_permissions(input_perms, asset1, 'output1')
    assert perms._sets == [set(), {asset_collection1c}]


def test_propagate_multiple_inputs(
        asset1, asset2, asset3, asset_collection1a, asset_collection2a,
        asset_category3a, asset_collection3a):

    policies = MockPolicies([
        ResultOfDataIn(asset1, asset3, 'output1', asset_collection1a),
        ResultOfDataIn(asset2, asset3, 'output1', asset_collection2a),
        InAssetCategory(asset1, asset_category3a),
        InAssetCategory(asset2, asset_category3a),
        ResultOfComputeIn(
            asset_category3a, asset3, 'output1', asset_collection3a)])
    evaluator = PolicyEvaluator(policies)

    input_perms = [Permissions([{asset1}]), Permissions([{asset2}])]
    perms = evaluator.propagate_permissions(input_perms, asset3, 'output1')
    assert perms._sets == [
        {asset_collection1a}, {asset_collection3a},
        {asset_collection2a}, {asset_collection3a}]


def test_propagate_multiple_outputs(
        asset1, asset2, asset_collection1a, asset_collection1b,
        asset_collection2a, asset_collection2b):
    policies = MockPolicies([
        ResultOfDataIn(asset1, asset2, 'output1', asset_collection1a),
        ResultOfDataIn(asset1, asset2, 'output2', asset_collection1b),
        ResultOfComputeIn(asset1, asset2, 'output1', asset_collection2a),
        ResultOfComputeIn(asset1, asset2, 'output2', asset_collection2b)])
    evaluator = PolicyEvaluator(policies)

    input_perms = [Permissions([{asset1}])]
    perms = evaluator.propagate_permissions(input_perms, asset2, 'output1')
    assert perms._sets == [{asset_collection1a}, {asset_collection2a}]

    perms = evaluator.propagate_permissions(input_perms, asset2, 'output2')
    assert perms._sets == [{asset_collection1b}, {asset_collection2b}]


def test_propagate_asset_wildcards(
        asset1, asset2, asset_collection1a, asset_collection2a, any_object):
    policies = MockPolicies([
        ResultOfDataIn(asset1, any_object, 'output1', asset_collection1a),
        ResultOfComputeIn(any_object, asset2, 'output1', asset_collection2a)])
    evaluator = PolicyEvaluator(policies)

    input_perms = [Permissions([{asset1}])]
    perms = evaluator.propagate_permissions(input_perms, asset2, 'output1')
    assert perms._sets == [{asset_collection1a}, {asset_collection2a}]


def test_propagate_output_wildcards(
        asset1, asset2, asset_collection1a, asset_collection2a):
    policies = MockPolicies([
        ResultOfDataIn(asset1, asset2, '*', asset_collection1a),
        ResultOfComputeIn(asset1, asset2, '*', asset_collection2a)])
    evaluator = PolicyEvaluator(policies)

    input_perms = [Permissions([{asset1}])]
    perms = evaluator.propagate_permissions(input_perms, asset2, 'output1')
    assert perms._sets == [{asset_collection1a}, {asset_collection2a}]
