from textwrap import indent
from typing import Any, Dict, List


from proof_of_concept.policy import (
        InAssetCollection, InPartyCollection, MayAccess, ResultOfIn)
from proof_of_concept.ddm_site import Site
from proof_of_concept.workflow import Job, WorkflowStep, Workflow


def run_scenario(scenario: Dict[str, Any]) -> Dict[str, Any]:
    # run
    print('Rules:')
    for rule in scenario['rules']:
        print('    {}'.format(rule))
    print()
    print('On behalf of: {}'.format(scenario['user_site'].administrator))
    print()
    print('Job:')
    print(indent(str(scenario['job']), ' '*4))
    print()

    result = scenario['user_site'].run_job(scenario['job'])
    print()
    print('Result:')
    print(result)
    return result


def test_pii(clean_global_registry):
    scenario = dict()     # type: Dict[str, Any]

    scenario['rules'] = [
            InAssetCollection('id:party1/dataset/pii1', 'PII1'),
            MayAccess('party1', 'PII1'),
            ResultOfIn('PII1', '*', 'PII1'),
            ResultOfIn('PII1', 'Anonymise', 'ScienceOnly1'),
            ResultOfIn('PII1', 'Aggregate', 'Public'),
            ResultOfIn('ScienceOnly1', '*', 'ScienceOnly1'),
            InAssetCollection('ScienceOnly1', 'ScienceOnly'),
            ResultOfIn('Public', '*', 'Public'),

            InAssetCollection('id:party2/dataset/pii2', 'PII2'),
            MayAccess('party2', 'PII2'),
            MayAccess('party1', 'PII2'),
            ResultOfIn('PII2', '*', 'PII2'),
            ResultOfIn('PII2', 'Anonymise', 'ScienceOnly2'),
            ResultOfIn('ScienceOnly2', '*', 'ScienceOnly2'),
            InAssetCollection('ScienceOnly2', 'ScienceOnly'),

            MayAccess('party3', 'ScienceOnly'),
            MayAccess('party1', 'Public'),
            MayAccess('party2', 'Public'),
            MayAccess('party3', 'Public'),
            ]

    scenario['sites'] = [
            Site(
                'site1', 'party1', {'id:party1/dataset/pii1': 42},
                scenario['rules']),
            Site(
                'site2', 'party2', {'id:party2/dataset/pii2': 3},
                scenario['rules']),
            Site('site3', 'party3', {}, scenario['rules'])]

    workflow = Workflow(
            ['x1', 'x2'], {'result': 'aggregate.y'}, [
                WorkflowStep(
                    'combine', {'x1': 'x1', 'x2': 'x2'}, ['y'], 'Combine'),
                WorkflowStep(
                    'anonymise', {'x1': 'combine.y'}, ['y'], 'Anonymise'),
                WorkflowStep(
                    'aggregate', {'x1': 'anonymise.y'}, ['y'], 'Aggregate')])

    inputs = {'x1': 'id:party1/dataset/pii1', 'x2': 'id:party2/dataset/pii2'}

    scenario['job'] = Job(workflow, inputs)
    scenario['user_site'] = scenario['sites'][2]

    output = run_scenario(scenario)
    assert output['result'] == 12.5


def test_saas_with_data(clean_global_registry):
    scenario = dict()     # type: Dict[str, Any]

    scenario['rules'] = [
            MayAccess('party1', 'id:party1/dataset/data1'),
            MayAccess('party2', 'id:party1/dataset/data1'),
            MayAccess('party2', 'id:party2/dataset/data2'),
            ResultOfIn('id:party1/dataset/data1', 'Addition', 'result1'),
            ResultOfIn('id:party2/dataset/data2', 'Addition', 'result2'),
            MayAccess('party2', 'result1'),
            MayAccess('party1', 'result1'),
            MayAccess('party1', 'result2'),
            MayAccess('party2', 'result2'),
            ]

    scenario['sites'] = [
            Site(
                'site1', 'party1', {'id:party1/dataset/data1': 42},
                scenario['rules']),
            Site(
                'site2', 'party2', {'id:party2/dataset/data2': 3},
                scenario['rules'])]

    workflow = Workflow(
            ['x1', 'x2'], {'y': 'addstep.y'}, [
                WorkflowStep(
                    'addstep', {'x1': 'x1', 'x2': 'x2'}, ['y'], 'Addition')
                ])

    inputs = {'x1': 'id:party1/dataset/data1', 'x2': 'id:party2/dataset/data2'}

    scenario['job'] = Job(workflow, inputs)
    scenario['user_site'] = scenario['sites'][0]

    output = run_scenario(scenario)
    assert output['y'] == 45
