from textwrap import indent
from typing import Any, Dict, List


from proof_of_concept.policy import (
        InAssetCollection, InPartyCollection, MayAccess, ResultOfIn)
from proof_of_concept.ddm_site import Site
from proof_of_concept.workflow import Job, WorkflowStep, Workflow


def run_scenario(scenario: Dict[str, Any]) -> Dict[str, Any]:
    # run
    print('Rules:')
    # for rule in scenario['rules']:
    #     print('    {}'.format(rule))
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

    scenario['rules-party1'] = [
            InAssetCollection(
                'id:party1/dataset/pii1', 'id:party1/collection/PII1'),
            MayAccess('party1', 'id:party1/collection/PII1'),
            ResultOfIn(
                'id:party1/collection/PII1', '*', 'id:party1/collection/PII1'),
            ResultOfIn(
                'id:party1/collection/PII1', 'Anonymise',
                'id:party1/collection/ScienceOnly1'),
            ResultOfIn(
                'id:party1/collection/PII1', 'Aggregate',
                'id:ddm/collection/Public'),
            ResultOfIn(
                'id:party1/collection/ScienceOnly1', '*',
                'id:party1/collection/ScienceOnly1'),
            InAssetCollection(
                'id:party1/collection/ScienceOnly1',
                'id:ddm/collection/ScienceOnly'),
            ]

    scenario['rules-party2'] = [
            ResultOfIn(
                'id:ddm/collection/Public', '*', 'id:ddm/collection/Public'),

            InAssetCollection(
                'id:party2/dataset/pii2', 'id:party2/collection/PII2'),
            MayAccess('party2', 'id:party2/collection/PII2'),
            MayAccess('party1', 'id:party2/collection/PII2'),
            ResultOfIn(
                'id:party2/collection/PII2', '*', 'id:party2/collection/PII2'),
            ResultOfIn(
                'id:party2/collection/PII2', 'Anonymise',
                'id:party2/collection/ScienceOnly2'),
            ResultOfIn(
                'id:party2/collection/ScienceOnly2', '*',
                'id:party2/collection/ScienceOnly2'),
            InAssetCollection(
                'id:party2/collection/ScienceOnly2',
                'id:ddm/collection/ScienceOnly'),
            ]

    scenario['rules-party3'] = [
            ResultOfIn(
                'id:ddm/collection/Public', '*', 'id:ddm/collection/Public'),

            MayAccess('party3', 'id:ddm/collection/ScienceOnly'),
            MayAccess('party1', 'id:ddm/collection/Public'),
            MayAccess('party2', 'id:ddm/collection/Public'),
            MayAccess('party3', 'id:ddm/collection/Public'),
            ]

    scenario['sites'] = [
            Site(
                'site1', 'party1', {'id:party1/dataset/pii1': 42},
                scenario['rules-party1']),
            Site(
                'site2', 'party2', {'id:party2/dataset/pii2': 3},
                scenario['rules-party2']),
            Site('site3', 'party3', {}, scenario['rules-party3'])]

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

    scenario['rules-party1'] = [
            MayAccess('party1', 'id:party1/dataset/data1'),
            MayAccess('party2', 'id:party1/dataset/data1'),
            ResultOfIn(
                'id:party1/dataset/data1', 'Addition',
                'id:party1/collection/result1'),
            MayAccess('party1', 'id:party1/collection/result1'),
            MayAccess('party2', 'id:party1/collection/result1'),
            ]

    scenario['rules-party2'] = [
            MayAccess('party2', 'id:party2/dataset/data2'),
            ResultOfIn(
                'id:party2/dataset/data2', 'Addition',
                'id:party2/collection/result2'),
            MayAccess('party1', 'id:party2/collection/result2'),
            MayAccess('party2', 'id:party2/collection/result2'),
            ]

    scenario['sites'] = [
            Site(
                'site1', 'party1', {'id:party1/dataset/data1': 42},
                scenario['rules-party1']),
            Site(
                'site2', 'party2', {'id:party2/dataset/data2': 3},
                scenario['rules-party2'])]

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
