import logging
from textwrap import indent
from typing import Any, Dict

from proof_of_concept.components.ddm_site import Site
from proof_of_concept.components.registry_client import RegistryClient
from proof_of_concept.definitions.assets import ComputeAsset, DataAsset
from proof_of_concept.definitions.workflows import Job, WorkflowStep, Workflow
from proof_of_concept.policy.rules import (
    InAssetCollection, MayAccess, ResultOfDataIn,
    ResultOfComputeIn)


logger = logging.getLogger(__file__)


def create_sites(
        registry_client: RegistryClient,
        site_descriptions: Dict[str, Any]
        ) -> Dict[str, Site]:
    return {
            site_name: Site(
                site_name, desc['owner'], desc['namespace'], desc['assets'],
                desc['rules'], registry_client)
            for site_name, desc in site_descriptions.items()}


def run_scenario(scenario: Dict[str, Any]) -> Dict[str, Any]:
    registry_client = RegistryClient()
    sites = create_sites(registry_client, scenario['sites'])

    logger.info('Running test scenario on behalf of {}'.format(
        scenario['user_site']))
    logger.info('Job:\n{}'.format(indent(str(scenario["job"]), " "*4)))

    result = sites[scenario['user_site']].run_job(scenario['job'])

    for site in sites.values():
        site.close()

    logger.info(f'Result: {result}')
    return result


def test_pii(registry_server):
    scenario = dict()     # type: Dict[str, Any]

    scenario['rules-party1'] = [
            InAssetCollection(
                'asset:party1_ns:dataset.pii1:party1_ns:site1',
                'asset_collection:party1_ns:collection.PII1'),
            MayAccess(
                'site:party1_ns:site1',
                'asset_collection:party1_ns:collection.PII1'),
            ResultOfDataIn(
                'asset_collection:party1_ns:collection.PII1', '*',
                'asset_collection:party1_ns:collection.PII1'),
            ResultOfDataIn(
                'asset_collection:party1_ns:collection.PII1',
                'asset:ddm_ns:software.anonymise:ddm_ns:site3',
                'asset_collection:party1_ns:collection.ScienceOnly1'),
            ResultOfDataIn(
                'asset_collection:party1_ns:collection.PII1',
                'asset:ddm_ns:software.aggregate:ddm_ns:site3',
                'asset_collection:ddm_ns:collection.Public'),
            ResultOfDataIn(
                'asset_collection:party1_ns:collection.ScienceOnly1', '*',
                'asset_collection:party1_ns:collection.ScienceOnly1'),
            InAssetCollection(
                'asset_collection:party1_ns:collection.ScienceOnly1',
                'asset_collection:ddm_ns:collection.ScienceOnly'),
            ]

    scenario['rules-party2'] = [
            InAssetCollection(
                'asset:party2_ns:dataset.pii2:party2_ns:site2',
                'asset_collection:party2_ns:collection.PII2'),
            MayAccess(
                'site:party2_ns:site2',
                'asset_collection:party2_ns:collection.PII2'),
            MayAccess(
                'site:party1_ns:site1',
                'asset_collection:party2_ns:collection.PII2'),
            ResultOfDataIn(
                'asset_collection:party2_ns:collection.PII2', '*',
                'asset_collection:party2_ns:collection.PII2'),
            ResultOfDataIn(
                'asset_collection:party2_ns:collection.PII2',
                'asset:ddm_ns:software.anonymise:ddm_ns:site3',
                'asset_collection:party2_ns:collection.ScienceOnly2'),
            ResultOfDataIn(
                'asset_collection:party2_ns:collection.ScienceOnly2', '*',
                'asset_collection:party2_ns:collection.ScienceOnly2'),
            InAssetCollection(
                'asset_collection:party2_ns:collection.ScienceOnly2',
                'asset_collection:ddm_ns:collection.ScienceOnly'),
            ]

    scenario['rules-ddm'] = [
            InAssetCollection(
                'asset:ddm_ns:software.anonymise:ddm_ns:site3',
                'asset_collection:ddm_ns:collection.PublicSoftware'),
            InAssetCollection(
                'asset:ddm_ns:software.aggregate:ddm_ns:site3',
                'asset_collection:ddm_ns:collection.PublicSoftware'),
            InAssetCollection(
                'asset:ddm_ns:software.combine:ddm_ns:site3',
                'asset_collection:ddm_ns:collection.PublicSoftware'),
            MayAccess(
                '*', 'asset_collection:ddm_ns:collection.PublicSoftware'),
            ResultOfDataIn(
                'asset_collection:ddm_ns:collection.Public', '*',
                'asset_collection:ddm_ns:collection.Public'),

            ResultOfComputeIn(
                '*', 'asset:ddm_ns:software.anonymise:ddm_ns:site3',
                'asset_collection:ddm_ns:collection.Public'),

            ResultOfComputeIn(
                '*', 'asset:ddm_ns:software.aggregate:ddm_ns:site3',
                'asset_collection:ddm_ns:collection.Public'),

            ResultOfComputeIn(
                '*', 'asset:ddm_ns:software.combine:ddm_ns:site3',
                'asset_collection:ddm_ns:collection.Public'),

            MayAccess(
                'site:ddm_ns:site3',
                'asset_collection:ddm_ns:collection.ScienceOnly'),
            MayAccess(
                'site:party1_ns:site1',
                'asset_collection:ddm_ns:collection.Public'),
            MayAccess(
                'site:party2_ns:site2',
                'asset_collection:ddm_ns:collection.Public'),
            MayAccess(
                'site:ddm_ns:site3',
                'asset_collection:ddm_ns:collection.Public'),
            ]

    scenario['sites'] = {
            'site1': {
                'owner': 'party:party1_ns:party1',
                'namespace': 'party1_ns',
                'assets': [
                    DataAsset(
                        'asset:party1_ns:dataset.pii1:party1_ns:site1', 42)],
                'rules': scenario['rules-party1']},
            'site2': {
                'owner': 'party:party2_ns:party2',
                'namespace': 'party2_ns',
                'assets': [
                    DataAsset(
                        'asset:party2_ns:dataset.pii2:party2_ns:site2', 3)],
                'rules': scenario['rules-party2']},
            'site3': {
                'owner': 'party:ddm_ns:ddm',
                'namespace': 'ddm_ns',
                'assets': [
                    ComputeAsset(
                        'asset:ddm_ns:software.combine:ddm_ns:site3',
                        None, None),
                    ComputeAsset(
                        'asset:ddm_ns:software.anonymise:ddm_ns:site3',
                        None, None),
                    ComputeAsset(
                        'asset:ddm_ns:software.aggregate:ddm_ns:site3',
                        None, None)],
                'rules': scenario['rules-ddm']}}

    workflow = Workflow(
            ['x1', 'x2'], {'result': 'aggregate.y'}, [
                WorkflowStep(
                    name='combine',
                    inputs={'x1': 'x1', 'x2': 'x2'},
                    outputs=['y'],
                    compute_asset_id=(
                        'asset:ddm_ns:software.combine:ddm_ns:site3')),
                WorkflowStep(
                    name='anonymise',
                    inputs={'x1': 'combine.y'},
                    outputs=['y'],
                    compute_asset_id=(
                        'asset:ddm_ns:software.anonymise:ddm_ns:site3')),
                WorkflowStep(
                    name='aggregate',
                    inputs={'x1': 'anonymise.y'},
                    outputs=['y'],
                    compute_asset_id=(
                        'asset:ddm_ns:software.aggregate:ddm_ns:site3'))
            ]
    )

    inputs = {
            'x1': 'asset:party1_ns:dataset.pii1:party1_ns:site1',
            'x2': 'asset:party2_ns:dataset.pii2:party2_ns:site2'}

    scenario['job'] = Job(workflow, inputs)
    scenario['user_site'] = 'site2'

    output = run_scenario(scenario)
    assert output['result'] == 12.5


def test_saas_with_data(registry_server):
    scenario = dict()     # type: Dict[str, Any]

    scenario['rules-party1'] = [
            MayAccess(
                'site:party1_ns:site1',
                'asset:party1_ns:dataset.data1:party1_ns:site1'),
            MayAccess(
                'site:party2_ns:site2',
                'asset:party1_ns:dataset.data1:party1_ns:site1'),
            ResultOfDataIn(
                'asset:party1_ns:dataset.data1:party1_ns:site1',
                'asset:party2_ns:software.addition:party2_ns:site2',
                'asset_collection:party1_ns:collection.result1'),
            MayAccess(
                'site:party1_ns:site1',
                'asset_collection:party1_ns:collection.result1'),
            MayAccess(
                'site:party2_ns:site2',
                'asset_collection:party1_ns:collection.result1'),
            ]

    scenario['rules-party2'] = [
            MayAccess(
                'site:party2_ns:site2',
                'asset:party2_ns:dataset.data2:party2_ns:site2'),
            MayAccess(
                'site:party2_ns:site2',
                'asset:party2_ns:software.addition:party2_ns:site2'),
            ResultOfDataIn(
                'asset:party2_ns:dataset.data2:party2_ns:site2',
                'asset:party2_ns:software.addition:party2_ns:site2',
                'asset_collection:party2_ns:collection.result2'),
            ResultOfComputeIn(
                'asset:party2_ns:dataset.data2:party2_ns:site2',
                'asset:party2_ns:software.addition:party2_ns:site2',
                'asset_collection:party2_ns:collection.result2'),
            ResultOfComputeIn(
                'asset:party1_ns:dataset.data1:party1_ns:site1',
                'asset:party2_ns:software.addition:party2_ns:site2',
                'asset_collection:party2_ns:collection.result2'),
            MayAccess(
                'site:party1_ns:site1',
                'asset_collection:party2_ns:collection.result2'),
            MayAccess(
                'site:party2_ns:site2',
                'asset_collection:party2_ns:collection.result2'),
            MayAccess(
                'site:party2_ns:site2',
                'asset:party2_ns:software.addition:party2_ns:site2'),
            ]

    scenario['sites'] = {
            'site1': {
                'owner': 'party:party1_ns:party1',
                'namespace': 'party1_ns',
                'assets': [
                    DataAsset(
                        'asset:party1_ns:dataset.data1:party1_ns:site1', 42)],
                'rules': scenario['rules-party1']},
            'site2': {
                'owner': 'party:party2_ns:party2',
                'namespace': 'party2_ns',
                'assets': [
                    DataAsset(
                        'asset:party2_ns:dataset.data2:party2_ns:site2', 3),
                    ComputeAsset(
                         'asset:party2_ns:software.addition:party2_ns:site2',
                         None, None)],
                'rules': scenario['rules-party2']}}

    workflow = Workflow(inputs=['x1', 'x2'],
                        outputs={'y': 'addstep.y'},
                        steps=[
                WorkflowStep(
                    name='addstep',
                    inputs={'x1': 'x1', 'x2': 'x2'},
                    outputs=['y'],
                    compute_asset_id=(
                        'asset:party2_ns:software.addition:party2_ns:site2'))
                ])

    inputs = {
            'x1': 'asset:party1_ns:dataset.data1:party1_ns:site1',
            'x2': 'asset:party2_ns:dataset.data2:party2_ns:site2'}

    scenario['job'] = Job(workflow, inputs)
    scenario['user_site'] = 'site1'

    output = run_scenario(scenario)
    assert output['y'] == 45
