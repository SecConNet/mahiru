from textwrap import indent
from typing import Any, Dict, List


from policy import (
        InAssetCollection, InPartyCollection, MayAccess, ResultOfIn)
from ddm_site import Site
from workflow import Job, WorkflowStep, Workflow


def run_scenario(scenario: Dict[str, Any]) -> None:
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


def scenario_saas_with_data() -> Dict[str, Any]:
    result = dict()     # type: Dict[str, Any]

    result['rules'] = [
            MayAccess('party1', 'site1-store:data1'),
            MayAccess('party2', 'site1-store:data1'),
            MayAccess('party2', 'site2-store:data2'),
            ResultOfIn('site1-store:data1', 'Addition', 'result1'),
            ResultOfIn('site2-store:data2', 'Addition', 'result2'),
            MayAccess('party2', 'result1'),
            MayAccess('party1', 'result1'),
            MayAccess('party1', 'result2'),
            MayAccess('party2', 'result2'),
            ]

    result['sites'] = [
            Site(
                'site1', 'party1', {'site1-store:data1': 42}, result['rules']),
            Site('site2', 'party2', {'site2-store:data2': 3}, result['rules'])]

    workflow = Workflow(
            ['x1', 'x2'], {'y': 'addstep/y'}, [
                WorkflowStep(
                    'addstep', {'x1': 'x1', 'x2': 'x2'}, ['y'], 'Addition')
                ])

    inputs = {'x1': 'site1-store:data1', 'x2': 'site2-store:data2'}

    result['job'] = Job(workflow, inputs)
    result['user_site'] = result['sites'][0]

    return result


def scenario_pii() -> Dict[str, Any]:
    scenario = dict()     # type: Dict[str, Any]

    scenario['rules'] = [
            InAssetCollection('site1-store:pii1', 'PII1'),
            MayAccess('party1', 'PII1'),
            ResultOfIn('PII1', '*', 'PII1'),
            ResultOfIn('PII1', 'Anonymise', 'ScienceOnly1'),
            ResultOfIn('PII1', 'Aggregate', 'Public'),
            ResultOfIn('ScienceOnly1', '*', 'ScienceOnly1'),
            InAssetCollection('ScienceOnly1', 'ScienceOnly'),
            ResultOfIn('Public', '*', 'Public'),

            InAssetCollection('site2-store:pii2', 'PII2'),
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
                'site1', 'party1', {'site1-store:pii1': 42},
                scenario['rules']),
            Site(
                'site2', 'party2', {'site2-store:pii2': 3}, scenario['rules']),
            Site('site3', 'party3', {}, scenario['rules'])]

    workflow = Workflow(
            ['x1', 'x2'], {'result': 'aggregate/y'}, [
                WorkflowStep(
                    'combine', {'x1': 'x1', 'x2': 'x2'}, ['y'], 'Combine'),
                WorkflowStep(
                    'anonymise', {'x1': 'combine/y'}, ['y'], 'Anonymise'),
                WorkflowStep(
                    'aggregate', {'x1': 'anonymise/y'}, ['y'], 'Aggregate')])

    inputs = {'x1': 'site1-store:pii1', 'x2': 'site2-store:pii2'}

    scenario['job'] = Job(workflow, inputs)
    scenario['user_site'] = scenario['sites'][2]

    return scenario


run_scenario(scenario_pii())
