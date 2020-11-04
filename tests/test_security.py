from unittest.mock import MagicMock

from proof_of_concept.asset import ComputeAsset
from proof_of_concept.policy import (
        MayAccess, PolicyEvaluator, ResultOfDataIn, ResultOfComputeIn)
from proof_of_concept.workflow import Job, Workflow, WorkflowStep
from proof_of_concept.workflow_engine import WorkflowPlanner


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
            's1' if 'p1' in x else 's2')

    rules = [
            MayAccess('s1', 'Anonymise'),
            MayAccess('s1', 'Aggregate'),
            ResultOfDataIn('Public', '*', 'Public'),
            MayAccess('s1', 'Public'),
            MayAccess('s2', 'Public'),
            MayAccess('s1', 'id:p1/dataset/d1'),
            ResultOfDataIn('id:p1/dataset/d1', 'Anonymise', 'Anonymous'),
            ResultOfComputeIn('*', 'Anonymise', 'Public'),
            MayAccess('s1', 'Anonymous'),
            ResultOfDataIn('Anonymous', 'Aggregate', 'Aggregated'),
            ResultOfComputeIn('*', 'Aggregate', 'Public'),
            MayAccess('s1', 'Aggregated'),
            MayAccess('s2', 'Aggregated'),
            ]
    policy_evaluator = PolicyEvaluator(MockPolicySource(rules))

    workflow = Workflow(
            ['x'], {'y': 'aggregate.y'},
            [
                WorkflowStep(name='anonymise',
                             inputs={'x1': 'x'},
                             outputs=['y'],
                             compute_asset_id='Anonymise'),
                WorkflowStep(name='aggregate',
                             inputs={'x1': 'anonymise.y'},
                             outputs=['y'],
                             compute_asset_id='Aggregate')])
    job = Job(workflow, {'x': 'id:p1/dataset/d1'})
    planner = WorkflowPlanner(mock_client, policy_evaluator)
    plans = planner.make_plans('s2', job)
    assert len(plans) == 1
    assert plans[0].input_sites['id:p1/dataset/d1'] == 's1'
    assert plans[0].step_sites[workflow.steps['anonymise']] == 's1'
    assert plans[0].step_sites[workflow.steps['aggregate']] == 's1'

    # test output from intermediate step
    workflow.outputs['y'] = 'anonymise.y'
    plans = planner.make_plans('p2', job)
    assert plans == []
