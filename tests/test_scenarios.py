import logging
from textwrap import indent
from typing import Any, Dict

from proof_of_concept.asset import ComputeAsset, DataAsset
from proof_of_concept.ddm_site import Site
from proof_of_concept.policy import (
    InAssetCollection, MayAccess, ResultOfDataIn,
    ResultOfComputeIn)
from proof_of_concept.workflow import Job, WorkflowStep, Workflow

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
                'id:party1_ns.dataset.pii1', 'id:party1_ns.collection.PII1'),
            MayAccess('party1', 'id:party1_ns.collection.PII1'),
            ResultOfDataIn(
                'id:party1_ns.collection.PII1', '*',
                'id:party1_ns.collection.PII1'),
            ResultOfDataIn(
                'id:party1_ns.collection.PII1', 'id:ddm_ns.software.anonymise',
                'id:party1_ns.collection.ScienceOnly1'),
            ResultOfDataIn(
                'id:party1_ns.collection.PII1', 'id:ddm_ns.software.aggregate',
                'id:ddm_ns.collection.Public'),
            ResultOfDataIn(
                'id:party1_ns.collection.ScienceOnly1', '*',
                'id:party1_ns.collection.ScienceOnly1'),
            InAssetCollection(
                'id:party1_ns.collection.ScienceOnly1',
                'id:ddm_ns.collection.ScienceOnly'),
            ]

    scenario['rules-party2'] = [
            InAssetCollection(
                'id:party2_ns.dataset.pii2', 'id:party2_ns.collection.PII2'),
            MayAccess('party2', 'id:party2_ns.collection.PII2'),
            MayAccess('party1', 'id:party2_ns.collection.PII2'),
            ResultOfDataIn(
                'id:party2_ns.collection.PII2', '*',
                'id:party2_ns.collection.PII2'),
            ResultOfDataIn(
                'id:party2_ns.collection.PII2', 'id:ddm_ns.software.anonymise',
                'id:party2_ns.collection.ScienceOnly2'),
            ResultOfDataIn(
                'id:party2_ns.collection.ScienceOnly2', '*',
                'id:party2_ns.collection.ScienceOnly2'),
            InAssetCollection(
                'id:party2_ns.collection.ScienceOnly2',
                'id:ddm_ns.collection.ScienceOnly'),
            ]

    scenario['rules-ddm'] = [
            InAssetCollection(
                'id:ddm_ns.software.anonymise',
                'id:ddm_ns.collection.PublicSoftware'),
            InAssetCollection(
                'id:ddm_ns.software.aggregate',
                'id:ddm_ns.collection.PublicSoftware'),
            InAssetCollection(
                'id:ddm_ns.software.combine',
                'id:ddm_ns.collection.PublicSoftware'),
            MayAccess('*', 'id:ddm_ns.collection.PublicSoftware'),
            ResultOfDataIn(
                'id:ddm_ns.collection.Public', '*',
                'id:ddm_ns.collection.Public'),

            ResultOfComputeIn(
                '*', 'id:ddm_ns.software.anonymise',
                'id:ddm_ns.collection.Public'),

            ResultOfComputeIn(
                '*', 'id:ddm_ns.software.aggregate',
                'id:ddm_ns.collection.Public'),

            ResultOfComputeIn(
                '*', 'id:ddm_ns.software.combine',
                'id:ddm_ns.collection.Public'),

            MayAccess('ddm', 'id:ddm_ns.collection.ScienceOnly'),
            MayAccess('party1', 'id:ddm_ns.collection.Public'),
            MayAccess('party2', 'id:ddm_ns.collection.Public'),
            MayAccess('ddm', 'id:ddm_ns.collection.Public'),
            ]

    scenario['sites'] = [
            Site(name='site1', owner='party1', namespace='party1_ns',
                 stored_data=[DataAsset('id:party1_ns.dataset.pii1', 42)],
                 rules=scenario['rules-party1']),
            Site(name='site2', owner='party2', namespace='party2_ns',
                 stored_data=[DataAsset('id:party2_ns.dataset.pii2', 3)],
                 rules=scenario['rules-party2']),
            Site(name='site3', owner='ddm', namespace='ddm_ns',
                 stored_data=[
                     ComputeAsset('id:ddm_ns.software.combine', None, None),
                     ComputeAsset('id:ddm_ns.software.anonymise', None, None),
                     ComputeAsset('id:ddm_ns.software.aggregate', None, None)],
                 rules=scenario['rules-ddm'])]

    workflow = Workflow(
            ['x1', 'x2'], {'result': 'aggregate.y'}, [
                WorkflowStep(name='combine',
                             inputs={'x1': 'x1', 'x2': 'x2'},
                             outputs=['y'],
                             compute_asset_id='id:ddm_ns.software.combine'),
                WorkflowStep(name='anonymise',
                             inputs={'x1': 'combine.y'},
                             outputs=['y'],
                             compute_asset_id='id:ddm_ns.software.anonymise'),
                WorkflowStep(name='aggregate',
                             inputs={'x1': 'anonymise.y'},
                             outputs=['y'],
                             compute_asset_id='id:ddm_ns.software.aggregate')
            ]
    )

    inputs = {
            'x1': 'id:party1_ns.dataset.pii1',
            'x2': 'id:party2_ns.dataset.pii2'}

    scenario['job'] = Job(workflow, inputs)
    scenario['user_site'] = scenario['sites'][1]

    output = run_scenario(scenario)
    assert output['result'] == 12.5


def test_saas_with_data(clean_global_registry, registry_server):
    scenario = dict()     # type: Dict[str, Any]

    scenario['rules-party1'] = [
            MayAccess('party1', 'id:party1_ns.dataset.data1'),
            MayAccess('party2', 'id:party1_ns.dataset.data1'),
            ResultOfDataIn(
                'id:party1_ns.dataset.data1', 'id:party2_ns.software.addition',
                'id:party1_ns.collection.result1'),
            MayAccess('party1', 'id:party1_ns.collection.result1'),
            MayAccess('party2', 'id:party1_ns.collection.result1'),
            ]

    scenario['rules-party2'] = [
            MayAccess('party2', 'id:party2_ns.dataset.data2'),
            MayAccess('party2', 'id:party2_ns.software.addition'),
            ResultOfDataIn(
                'id:party2_ns.dataset.data2', 'id:party2_ns.software.addition',
                'id:party2_ns.collection.result2'),
            ResultOfComputeIn(
                'id:party2_ns.dataset.data2', 'id:party2_ns.software.addition',
                'id:party2_ns.collection.result2'),
            ResultOfComputeIn(
                'id:party1_ns.dataset.data1', 'id:party2_ns.software.addition',
                'id:party2_ns.collection.result2'),
            MayAccess('party1', 'id:party2_ns.collection.result2'),
            MayAccess('party2', 'id:party2_ns.collection.result2'),
            MayAccess('party2', 'id:party2_ns.software.addition'),
            ]

    scenario['sites'] = [
            Site(
                'site1', 'party1', 'party1_ns',
                [DataAsset('id:party1_ns.dataset.data1', 42)],
                scenario['rules-party1']),
            Site(
                'site2', 'party2', 'party2_ns',
                [DataAsset('id:party2_ns.dataset.data2', 3),
                 ComputeAsset('id:party2_ns.software.addition', None, None)],
                scenario['rules-party2'])]

    workflow = Workflow(inputs=['x1', 'x2'],
                        outputs={'y': 'addstep.y'},
                        steps=[
                WorkflowStep(
                    name='addstep',
                    inputs={'x1': 'x1', 'x2': 'x2'},
                    outputs=['y'],
                    compute_asset_id='id:party2_ns.software.addition')
                ])

    inputs = {
            'x1': 'id:party1_ns.dataset.data1',
            'x2': 'id:party2_ns.dataset.data2'}

    scenario['job'] = Job(workflow, inputs)
    scenario['user_site'] = scenario['sites'][0]

    output = run_scenario(scenario)
    assert output['y'] == 45
