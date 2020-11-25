from unittest.mock import MagicMock

from proof_of_concept.definitions.assets import ComputeAsset
from proof_of_concept.definitions.workflows import Job, Workflow, WorkflowStep
from proof_of_concept.policy.evaluation import PolicyEvaluator
from proof_of_concept.components.orchestration import WorkflowPlanner
from proof_of_concept.policy.rules import (
        MayAccess, ResultOfDataIn, ResultOfComputeIn)


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
    mock_client.list_sites_with_runners = MagicMock(return_value=['s1', 's2'])
    mock_client.get_asset_location = lambda x: (
            's1' if 'ns1' in x else 's2')

    rules = [
            MayAccess('s1', 'id:ns:Anonymise'),
            MayAccess('s1', 'id:ns:Aggregate'),
            ResultOfDataIn('id:ns:Public', '*', 'id:ns:Public'),
            MayAccess('s1', 'id:ns:Public'),
            MayAccess('s2', 'id:ns:Public'),
            MayAccess('s1', 'id:ns1:dataset.d1:s1'),
            ResultOfDataIn(
                'id:ns1:dataset.d1:s1', 'id:ns:Anonymise', 'id:ns1:Anonymous'),
            ResultOfComputeIn('*', 'id:ns:Anonymise', 'id:ns:Public'),
            MayAccess('s1', 'id:ns1:Anonymous'),
            ResultOfDataIn(
                'id:ns1:Anonymous', 'id:ns:Aggregate', 'id:ns1:Aggregated'),
            ResultOfComputeIn('*', 'id:ns:Aggregate', 'id:ns:Public'),
            MayAccess('s1', 'id:ns1:Aggregated'),
            MayAccess('s2', 'id:ns1:Aggregated'),
            ]
    policy_evaluator = PolicyEvaluator(MockPolicySource(rules))

    workflow = Workflow(
            ['x'], {'y': 'aggregate.y'},
            [
                WorkflowStep(name='anonymise',
                             inputs={'x1': 'x'},
                             outputs=['y'],
                             compute_asset_id='id:ns:Anonymise'),
                WorkflowStep(name='aggregate',
                             inputs={'x1': 'anonymise.y'},
                             outputs=['y'],
                             compute_asset_id='id:ns:Aggregate')])
    job = Job(workflow, {'x': 'id:ns1:dataset.d1:s1'})
    planner = WorkflowPlanner(mock_client, policy_evaluator)
    plans = planner.make_plans('s2', job)
    assert len(plans) == 1
    assert plans[0].step_sites['anonymise'] == 's1'
    assert plans[0].step_sites['aggregate'] == 's1'

    # test output from intermediate step
    workflow.outputs['y'] = 'anonymise.y'
    plans = planner.make_plans('p2', job)
    assert plans == []
