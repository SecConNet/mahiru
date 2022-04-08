from unittest.mock import MagicMock

from mahiru.definitions.assets import ComputeAsset
from mahiru.definitions.identifier import Identifier
from mahiru.definitions.workflows import Job, Workflow, WorkflowStep
from mahiru.policy.evaluation import PolicyEvaluator
from mahiru.components.orchestration import WorkflowPlanner
from mahiru.policy.rules import (
        MayAccess, MayUse, ResultOfDataIn, ResultOfComputeIn)


class MockPolicySource:
    def __init__(self, rules):
        self._rules = rules

    def policies(self):
        return self._rules

    def update(self):
        pass


def test_wf_output_checks():
    """Check whether workflow output permissions are checked."""
    mock_client = MagicMock()
    mock_client.list_sites_with_runners = MagicMock(return_value=[
        'site:ns1:s1', 'site:ns2:s2'])
    mock_client.get_asset_location = lambda x: (
            'site:ns1:s1' if 'ns1' in x else 'site:ns2:s2')

    rules = [
            MayAccess('site:ns1:s1', 'asset:ns:Anonymise:ns:s'),
            MayAccess('site:ns1:s1', 'asset:ns:Aggregate:ns:s'),

            ResultOfDataIn(
                'asset_collection:ns:Public', '*', '*',
                'asset_collection:ns:Public'),
            MayAccess('site:ns1:s1', 'asset_collection:ns:Public'),
            MayAccess('site:ns2:s2', 'asset_collection:ns:Public'),
            MayUse('party:ns2:party2', 'asset_collection:ns:Public', ''),

            MayAccess('site:ns1:s1', 'asset:ns1:dataset.d1:ns1:s1'),
            ResultOfDataIn(
                'asset:ns1:dataset.d1:ns1:s1', 'asset:ns:Anonymise:ns:s', 'y',
                'asset_collection:ns1:Anonymous'),
            ResultOfComputeIn(
                '*', 'asset:ns:Anonymise:ns:s', 'y',
                'asset_collection:ns:Public'),
            MayAccess('site:ns1:s1', 'asset_collection:ns1:Anonymous'),
            ResultOfDataIn(
                'asset_collection:ns1:Anonymous', 'asset:ns:Aggregate:ns:s',
                'y', 'asset_collection:ns1:Aggregated'),
            ResultOfComputeIn(
                '*', 'asset:ns:Aggregate:ns:s', 'y',
                'asset_collection:ns:Public'),
            MayAccess('site:ns1:s1', 'asset_collection:ns1:Aggregated'),
            MayAccess('site:ns2:s2', 'asset_collection:ns1:Aggregated'),
            MayUse('party:ns2:party2', 'asset_collection:ns1:Aggregated', ''),
            ]
    policy_evaluator = PolicyEvaluator(MockPolicySource(rules))

    workflow = Workflow(
            ['x'], {'y': 'aggregate.y'},
            [
                WorkflowStep(name='anonymise',
                             inputs={'x1': 'x'},
                             outputs={'y': None},
                             compute_asset_id='asset:ns:Anonymise:ns:s'),
                WorkflowStep(name='aggregate',
                             inputs={'x1': 'anonymise.y'},
                             outputs={'y': None},
                             compute_asset_id='asset:ns:Aggregate:ns:s')])
    job = Job(
            Identifier('party:ns2:party2'), workflow,
            {'x': 'asset:ns1:dataset.d1:ns1:s1'})
    planner = WorkflowPlanner(mock_client, policy_evaluator)
    plans = planner.make_plans('party:ns2:party2', 'site:ns2:s2', job)
    assert len(plans) == 1
    assert plans[0].step_sites['anonymise'] == 'site:ns1:s1'
    assert plans[0].step_sites['aggregate'] == 'site:ns1:s1'

    # test output from intermediate step
    workflow.outputs['y'] = 'anonymise.y'
    plans = planner.make_plans('party:ns2:party2', 'site:ns2:s2', job)
    assert plans == []

    # test that use permission is needed
    workflow.outputs['y'] = 'aggregate.y'
    plans = planner.make_plans('party:ns1:party1', 'site:ns1:s1', job)
    assert plans == []
