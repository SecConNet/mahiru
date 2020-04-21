from textwrap import indent
from typing import Any, Dict, List


from proof_of_concept.policy import (
        InAssetCollection, InPartyCollection, MayAccess, ResultOfDataIn)
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
                'id:party1_ns/dataset/pii1', 'id:party1_ns/collection/PII1'),
            MayAccess('party1', 'id:party1_ns/collection/PII1'),
            ResultOfDataIn(
                'id:party1_ns/collection/PII1', '*',
                'id:party1_ns/collection/PII1'),
            ResultOfDataIn(
                'id:party1_ns/collection/PII1', 'Anonymise',
                'id:party1_ns/collection/ScienceOnly1'),
            ResultOfDataIn(
                'id:party1_ns/collection/PII1', 'Aggregate',
                'id:ddm_ns/collection/Public'),
            ResultOfDataIn(
                'id:party1_ns/collection/ScienceOnly1', '*',
                'id:party1_ns/collection/ScienceOnly1'),
            InAssetCollection(
                'id:party1_ns/collection/ScienceOnly1',
                'id:ddm_ns/collection/ScienceOnly'),
            ]

    scenario['rules-party2'] = [
            InAssetCollection(
                'id:party2_ns/dataset/pii2', 'id:party2_ns/collection/PII2'),
            MayAccess('party2', 'id:party2_ns/collection/PII2'),
            MayAccess('party1', 'id:party2_ns/collection/PII2'),
            ResultOfDataIn(
                'id:party2_ns/collection/PII2', '*',
                'id:party2_ns/collection/PII2'),
            ResultOfDataIn(
                'id:party2_ns/collection/PII2', 'Anonymise',
                'id:party2_ns/collection/ScienceOnly2'),
            ResultOfDataIn(
                'id:party2_ns/collection/ScienceOnly2', '*',
                'id:party2_ns/collection/ScienceOnly2'),
            InAssetCollection(
                'id:party2_ns/collection/ScienceOnly2',
                'id:ddm_ns/collection/ScienceOnly'),
            ]

    scenario['rules-ddm'] = [
            ResultOfDataIn(
                'id:ddm_ns/collection/Public', '*',
                'id:ddm_ns/collection/Public'),

            MayAccess('ddm', 'id:ddm_ns/collection/ScienceOnly'),
            MayAccess('party1', 'id:ddm_ns/collection/Public'),
            MayAccess('party2', 'id:ddm_ns/collection/Public'),
            MayAccess('ddm', 'id:ddm_ns/collection/Public'),
            ]

    scenario['sites'] = [
            Site(
                'site1', 'party1', 'party1_ns',
                {'id:party1_ns/dataset/pii1': 42}, scenario['rules-party1']),
            Site(
                'site2', 'party2', 'party2_ns',
                {'id:party2_ns/dataset/pii2': 3}, scenario['rules-party2']),
            Site('site3', 'ddm', 'ddm_ns', {}, scenario['rules-ddm'])]

    workflow = Workflow(
            ['x1', 'x2'], {'result': 'aggregate.y'}, [
                WorkflowStep(
                    'combine', {'x1': 'x1', 'x2': 'x2'}, ['y'], 'Combine'),
                WorkflowStep(
                    'anonymise', {'x1': 'combine.y'}, ['y'], 'Anonymise'),
                WorkflowStep(
                    'aggregate', {'x1': 'anonymise.y'}, ['y'], 'Aggregate')])

    inputs = {
            'x1': 'id:party1_ns/dataset/pii1',
            'x2': 'id:party2_ns/dataset/pii2'}

    scenario['job'] = Job(workflow, inputs)
    scenario['user_site'] = scenario['sites'][1]

    output = run_scenario(scenario)
    assert output['result'] == 12.5


def test_saas_with_data(clean_global_registry):
    scenario = dict()     # type: Dict[str, Any]

    scenario['rules-party1'] = [
            MayAccess('party1', 'id:party1_ns/dataset/data1'),
            MayAccess('party2', 'id:party1_ns/dataset/data1'),
            ResultOfDataIn(
                'id:party1_ns/dataset/data1', 'Addition',
                'id:party1_ns/collection/result1'),
            MayAccess('party1', 'id:party1_ns/collection/result1'),
            MayAccess('party2', 'id:party1_ns/collection/result1'),
            ]

    scenario['rules-party2'] = [
            MayAccess('party2', 'id:party2_ns/dataset/data2'),
            ResultOfDataIn(
                'id:party2_ns/dataset/data2', 'Addition',
                'id:party2_ns/collection/result2'),
            MayAccess('party1', 'id:party2_ns/collection/result2'),
            MayAccess('party2', 'id:party2_ns/collection/result2'),
            ]

    scenario['sites'] = [
            Site(
                'site1', 'party1', 'party1_ns',
                {'id:party1_ns/dataset/data1': 42}, scenario['rules-party1']),
            Site(
                'site2', 'party2', 'party2_ns',
                {'id:party2_ns/dataset/data2': 3}, scenario['rules-party2'])]

    workflow = Workflow(
            ['x1', 'x2'], {'y': 'addstep.y'}, [
                WorkflowStep(
                    'addstep', {'x1': 'x1', 'x2': 'x2'}, ['y'], 'Addition')
                ])

    inputs = {
            'x1': 'id:party1_ns/dataset/data1',
            'x2': 'id:party2_ns/dataset/data2'}

    scenario['job'] = Job(workflow, inputs)
    scenario['user_site'] = scenario['sites'][0]

    output = run_scenario(scenario)
    assert output['y'] == 45
