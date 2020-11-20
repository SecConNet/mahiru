import logging
from textwrap import indent
from typing import Any, Dict

from proof_of_concept.components.ddm_site import Site
from proof_of_concept.definitions.assets import ComputeAsset, DataAsset
from proof_of_concept.definitions.workflows import Job, WorkflowStep, Workflow
from proof_of_concept.policy.rules import (
    InAssetCollection, MayAccess, ResultOfDataIn,
    ResultOfComputeIn)


logger = logging.getLogger(__file__)


def run_scenario(scenario: Dict[str, Any]) -> Dict[str, Any]:
    logger.info('Running test scenario '
                'on behalf of: {}'.format(scenario['user_site'].owner))
    logger.info(f'Job:\n'
                f'{indent(str(scenario["job"]), " "*4)}')

    result = scenario['user_site'].run_job(scenario['job'])
    for site in scenario['sites']:
        site.close()

    logger.info(f'Result: {result}')
    return result


def test_pii(clean_global_registry, registry_server):
    scenario = dict()     # type: Dict[str, Any]

    scenario['rules-party1'] = [
            InAssetCollection(
                'id:party1_ns:dataset.pii1:site1',
                'id:party1_ns:collection.PII1'),
            MayAccess('site1', 'id:party1_ns:collection.PII1'),
            ResultOfDataIn(
                'id:party1_ns:collection.PII1', '*',
                'id:party1_ns:collection.PII1'),
            ResultOfDataIn(
                'id:party1_ns:collection.PII1',
                'id:ddm_ns:software.anonymise:site3',
                'id:party1_ns:collection.ScienceOnly1'),
            ResultOfDataIn(
                'id:party1_ns:collection.PII1',
                'id:ddm_ns:software.aggregate:site3',
                'id:ddm_ns:collection.Public'),
            ResultOfDataIn(
                'id:party1_ns:collection.ScienceOnly1', '*',
                'id:party1_ns:collection.ScienceOnly1'),
            InAssetCollection(
                'id:party1_ns:collection.ScienceOnly1',
                'id:ddm_ns:collection.ScienceOnly'),
            ]

    scenario['rules-party2'] = [
            InAssetCollection(
                'id:party2_ns:dataset.pii2:site2',
                'id:party2_ns:collection.PII2'),
            MayAccess('site2', 'id:party2_ns:collection.PII2'),
            MayAccess('site1', 'id:party2_ns:collection.PII2'),
            ResultOfDataIn(
                'id:party2_ns:collection.PII2', '*',
                'id:party2_ns:collection.PII2'),
            ResultOfDataIn(
                'id:party2_ns:collection.PII2',
                'id:ddm_ns:software.anonymise:site3',
                'id:party2_ns:collection.ScienceOnly2'),
            ResultOfDataIn(
                'id:party2_ns:collection.ScienceOnly2', '*',
                'id:party2_ns:collection.ScienceOnly2'),
            InAssetCollection(
                'id:party2_ns:collection.ScienceOnly2',
                'id:ddm_ns:collection.ScienceOnly'),
            ]

    scenario['rules-ddm'] = [
            InAssetCollection(
                'id:ddm_ns:software.anonymise:site3',
                'id:ddm_ns:collection.PublicSoftware'),
            InAssetCollection(
                'id:ddm_ns:software.aggregate:site3',
                'id:ddm_ns:collection.PublicSoftware'),
            InAssetCollection(
                'id:ddm_ns:software.combine:site3',
                'id:ddm_ns:collection.PublicSoftware'),
            MayAccess('*', 'id:ddm_ns:collection.PublicSoftware'),
            ResultOfDataIn(
                'id:ddm_ns:collection.Public', '*',
                'id:ddm_ns:collection.Public'),

            ResultOfComputeIn(
                '*', 'id:ddm_ns:software.anonymise:site3',
                'id:ddm_ns:collection.Public'),

            ResultOfComputeIn(
                '*', 'id:ddm_ns:software.aggregate:site3',
                'id:ddm_ns:collection.Public'),

            ResultOfComputeIn(
                '*', 'id:ddm_ns:software.combine:site3',
                'id:ddm_ns:collection.Public'),

            MayAccess('site3', 'id:ddm_ns:collection.ScienceOnly'),
            MayAccess('site1', 'id:ddm_ns:collection.Public'),
            MayAccess('site2', 'id:ddm_ns:collection.Public'),
            MayAccess('site3', 'id:ddm_ns:collection.Public'),
            ]

    scenario['sites'] = [
            Site(name='site1', owner='party1', namespace='party1_ns',
                 stored_data=[
                     DataAsset('id:party1_ns:dataset.pii1:site1', 42)],
                 rules=scenario['rules-party1']),
            Site(name='site2', owner='party2', namespace='party2_ns',
                 stored_data=[
                     DataAsset('id:party2_ns:dataset.pii2:site2', 3)],
                 rules=scenario['rules-party2']),
            Site(name='site3', owner='ddm', namespace='ddm_ns',
                 stored_data=[
                     ComputeAsset(
                         'id:ddm_ns:software.combine:site3', None, None),
                     ComputeAsset(
                         'id:ddm_ns:software.anonymise:site3', None, None),
                     ComputeAsset(
                         'id:ddm_ns:software.aggregate:site3', None, None)],
                 rules=scenario['rules-ddm'])]

    workflow = Workflow(
            ['x1', 'x2'], {'result': 'aggregate.y'}, [
                WorkflowStep(
                    name='combine',
                    inputs={'x1': 'x1', 'x2': 'x2'},
                    outputs=['y'],
                    compute_asset_id='id:ddm_ns:software.combine:site3'),
                WorkflowStep(
                    name='anonymise',
                    inputs={'x1': 'combine.y'},
                    outputs=['y'],
                    compute_asset_id='id:ddm_ns:software.anonymise:site3'),
                WorkflowStep(
                    name='aggregate',
                    inputs={'x1': 'anonymise.y'},
                    outputs=['y'],
                    compute_asset_id='id:ddm_ns:software.aggregate:site3')
            ]
    )

    inputs = {
            'x1': 'id:party1_ns:dataset.pii1:site1',
            'x2': 'id:party2_ns:dataset.pii2:site2'}

    scenario['job'] = Job(workflow, inputs)
    scenario['user_site'] = scenario['sites'][1]

    output = run_scenario(scenario)
    assert output['result'] == 12.5


def test_saas_with_data(clean_global_registry, registry_server):
    scenario = dict()     # type: Dict[str, Any]

    scenario['rules-party1'] = [
            MayAccess('site1', 'id:party1_ns:dataset.data1:site1'),
            MayAccess('site2', 'id:party1_ns:dataset.data1:site1'),
            ResultOfDataIn(
                'id:party1_ns:dataset.data1:site1',
                'id:party2_ns:software.addition:site2',
                'id:party1_ns:collection.result1'),
            MayAccess('site1', 'id:party1_ns:collection.result1'),
            MayAccess('site2', 'id:party1_ns:collection.result1'),
            ]

    scenario['rules-party2'] = [
            MayAccess('site2', 'id:party2_ns:dataset.data2:site2'),
            MayAccess('site2', 'id:party2_ns:software.addition:site2'),
            ResultOfDataIn(
                'id:party2_ns:dataset.data2:site2',
                'id:party2_ns:software.addition:site2',
                'id:party2_ns:collection.result2'),
            ResultOfComputeIn(
                'id:party2_ns:dataset.data2:site2',
                'id:party2_ns:software.addition:site2',
                'id:party2_ns:collection.result2'),
            ResultOfComputeIn(
                'id:party1_ns:dataset.data1:site1',
                'id:party2_ns:software.addition:site2',
                'id:party2_ns:collection.result2'),
            MayAccess('site1', 'id:party2_ns:collection.result2'),
            MayAccess('site2', 'id:party2_ns:collection.result2'),
            MayAccess('site2', 'id:party2_ns:software.addition:site2'),
            ]

    scenario['sites'] = [
            Site(
                'site1', 'party1', 'party1_ns',
                [DataAsset('id:party1_ns:dataset.data1:site1', 42)],
                scenario['rules-party1']),
            Site(
                'site2', 'party2', 'party2_ns',
                [DataAsset('id:party2_ns:dataset.data2:site2', 3),
                 ComputeAsset(
                     'id:party2_ns:software.addition:site2', None, None)],
                scenario['rules-party2'])]

    workflow = Workflow(inputs=['x1', 'x2'],
                        outputs={'y': 'addstep.y'},
                        steps=[
                WorkflowStep(
                    name='addstep',
                    inputs={'x1': 'x1', 'x2': 'x2'},
                    outputs=['y'],
                    compute_asset_id='id:party2_ns:software.addition:site2')
                ])

    inputs = {
            'x1': 'id:party1_ns:dataset.data1:site1',
            'x2': 'id:party2_ns:dataset.data2:site2'}

    scenario['job'] = Job(workflow, inputs)
    scenario['user_site'] = scenario['sites'][0]

    output = run_scenario(scenario)
    assert output['y'] == 45
