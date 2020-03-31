from unittest.mock import MagicMock

from proof_of_concept.policy import MayAccess, PolicyEvaluator, ResultOfIn
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
    mock_client.list_runners = MagicMock(return_value=['s1', 's2'])
    mock_client.get_runner_administrator = lambda x: (
            'p1' if x == 's1' else 'p2')
    mock_client.get_asset_location = lambda x: (
            's1-store' if 'p1' in x else 's2-store')

    rules = [
            MayAccess('p1', 'id:p1/dataset/d1'),
            ResultOfIn('id:p1/dataset/d1', 'Anonymise', 'Anonymous'),
            MayAccess('p1', 'Anonymous'),
            ResultOfIn('Anonymous', 'Aggregate', 'Aggregated'),
            MayAccess('p1', 'Aggregated'),
            MayAccess('p2', 'Aggregated'),
            ]
    policy_evaluator = PolicyEvaluator(MockPolicySource(rules))

    workflow = Workflow(
            ['x'], {'y': 'aggregate.y'},
            [
                WorkflowStep('anonymise', {'x1': 'x'}, ['y'], 'Anonymise'),
                WorkflowStep(
                    'aggregate', {'x1': 'anonymise.y'}, ['y'], 'Aggregate')])
    job = Job(workflow, {'x': 'id:p1/dataset/d1'})
    planner = WorkflowPlanner(mock_client, policy_evaluator)
    plans = planner.make_plans('p2', job)
    assert len(plans) == 1
    assert plans[0].input_stores['id:p1/dataset/d1'] == 's1-store'
    assert plans[0].step_runners[workflow.steps['anonymise']] == 's1'
    assert plans[0].step_runners[workflow.steps['aggregate']] == 's1'

    # test output from intermediate step
    workflow.outputs['y'] = 'anonymise.y'
    plans = planner.make_plans('p2', job)
    assert plans == []
