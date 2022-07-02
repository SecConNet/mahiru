import logging
from textwrap import indent
import time
from typing import Any, Dict

import requests

from mahiru.components.ddm_site import Site
from mahiru.components.registry_client import RegistryClient
from mahiru.components.settings import NetworkSettings, SiteConfiguration
from mahiru.definitions.assets import ComputeAsset, DataAsset
from mahiru.definitions.identifier import Identifier
from mahiru.definitions.registry import PartyDescription, SiteDescription
from mahiru.definitions.workflows import Job, WorkflowStep, Workflow
from mahiru.policy.rules import (
    InAssetCollection, MayAccess, MayUse, ResultOfDataIn,
    ResultOfComputeIn)
from mahiru.rest.ddm_site import SiteRestApi, SiteServer
from mahiru.rest.internal_client import InternalSiteRestClient
from mahiru.rest.registry_client import RegistrationRestClient


logger = logging.getLogger(__file__)


def register_parties(
        registration_client: RegistrationRestClient,
        parties: Dict[str, Any],
        ) -> None:
    """Register parties with their public keys."""
    for party_id, party in parties.items():
        registration_client.register_party(
                PartyDescription(
                    party_id, party['namespace'],
                    party['main_certificate'],
                    party['user_ca_certificate'], []))


def sign_rules(
        site_descriptions: Dict[str, Any], parties: Dict[str, Any]
        ) -> None:
    """Update site descriptions by signing rules."""
    for desc in site_descriptions.values():
        private_key = parties[desc['owner']]['main_key']
        for rule in desc['rules']:
            rule.sign(private_key)


def create_sites(
        registry_client: RegistryClient,
        site_descriptions: Dict[str, Any]
        ) -> Dict[str, Site]:
    """Creates sites for the scenario."""
    return {
            site_id: Site(
                SiteConfiguration(
                    site_id, desc['namespace'], Identifier(desc['owner']),
                    NetworkSettings(), ''),
                [], [], registry_client)
            for site_id, desc in site_descriptions.items()}


def create_servers(sites: Dict[str, Site]) -> Dict[str, SiteServer]:
    """Create REST servers for sites."""
    servers = {
            site_name: SiteServer(
                    SiteRestApi(
                        site.policy_store, site.store, site.runner,
                        site.orchestrator))
            for site_name, site in sites.items()}

    # wait for them to come up
    for server in servers.values():
        requests.get(server.internal_endpoint, timeout=(600.0, 1.0))

    return servers


def create_clients(servers: Dict[str, SiteServer], sites: Dict[str, Site]):
    """Create internal REST clients for sites."""
    return {
            site_name: InternalSiteRestClient(
                sites[site_name].owner, sites[site_name].id,
                server.internal_endpoint)
            for site_name, server in servers.items()}


def upload_assets(
        site_descriptions: Dict[str, Any], clients: Dict[str, Site]) -> None:
    """Add assets to sites using internal API."""
    for site_name, desc in site_descriptions.items():
        for asset in desc['assets']:
            clients[site_name].store_asset(asset)


def add_rules(
        site_descriptions: Dict[str, Any], clients: Dict[str, Site]) -> None:
    """Add rules to sites using internal API."""
    for site_name, desc in site_descriptions.items():
        for rule in desc['rules']:
            clients[site_name].add_rule(rule)


def register_sites(
        site_descs: Dict[str, Any],
        registration_client: RegistrationRestClient, sites: Dict[str, Site],
        servers: Dict[str, SiteServer]) -> None:
    """Register sites with the registry."""
    for site_name, site in sites.items():
        registration_client.register_site(
                SiteDescription(
                    site.id, site.owner, site.administrator,
                    servers[site_name].external_endpoint,
                    site_descs[site.id]['https_certificate'],
                    True, True, True))


def stop_servers(servers: Dict[str, SiteServer]):
    """Stops the sites' REST servers."""
    for server in servers.values():
        server.close()


def deregister_sites(
        registration_client: RegistrationRestClient, sites: Dict[str, Site]
        ) -> None:
    """Deregisters sites from the registry."""
    for site in sites.values():
        registration_client.deregister_site(site.id)


def deregister_parties(
        registration_client: RegistrationRestClient,
        parties: Dict[str, str]
        ) -> None:
    """Deregisters parties from the registry."""
    for party in parties:
        registration_client.deregister_party(party)


def run_scenario(
        scenario: Dict[str, Any], registry_client: RegistryClient,
        registration_client: RegistrationRestClient
        ) -> Dict[str, Any]:
    logger.info('Running test scenario on behalf of {}'.format(
        scenario['user_site']))
    logger.info('Job:\n{}'.format(indent(str(scenario["job"]), " "*4)))

    register_parties(registration_client, scenario['parties'])

    sign_rules(scenario['sites'], scenario['parties'])
    sites = create_sites(registry_client, scenario['sites'])
    servers = create_servers(sites)
    clients = create_clients(servers, sites)
    upload_assets(scenario['sites'], clients)
    add_rules(scenario['sites'], clients)
    register_sites(scenario['sites'], registration_client, sites, servers)

    client = clients[scenario['user_site']]
    job_id = client.submit_job(scenario['job'])
    while not client.is_job_done(job_id):
        time.sleep(0.1)
    result = client.get_job_result(job_id)

    stop_servers(servers)
    deregister_sites(registration_client, sites)
    deregister_parties(registration_client, scenario['parties'])

    logger.info(f'Result: {result.outputs}')
    return result.outputs


def test_pii(
        registry_server, registry_client, registration_client,
        party1_main_certificate, party1_user_ca_certificate,
        party1_main_key, site1_https_certificate,
        party2_main_certificate, party2_user_ca_certificate,
        party2_main_key, site2_https_certificate,
        party3_main_certificate, party3_user_ca_certificate,
        party3_main_key, site3_https_certificate):

    scenario = dict()     # type: Dict[str, Any]

    scenario['parties'] = {
            'party:party1.mahiru.example.org:party1': {
                'namespace': 'party1.mahiru.example.org',
                'main_certificate': party1_main_certificate,
                'main_key': party1_main_key,
                'user_ca_certificate': party1_user_ca_certificate,
                'user_certificates': []},
            'party:party2.mahiru.example.org:party2': {
                'namespace': 'party2.mahiru.example.org',
                'main_certificate': party2_main_certificate,
                'main_key': party2_main_key,
                'user_ca_certificate': party2_user_ca_certificate,
                'user_certificates': []},
            'party:party3.mahiru.example.org:party3': {
                'namespace': 'party3.mahiru.example.org',
                'main_certificate': party3_main_certificate,
                'main_key': party3_main_key,
                'user_ca_certificate': party3_user_ca_certificate,
                'user_certificates': []},
            }

    scenario['rules-party1'] = [
            InAssetCollection(
                'asset:party1.mahiru.example.org:dataset.pii1'
                ':party1.mahiru.example.org:site1',
                'asset_collection:party1.mahiru.example.org:collection.PII1'),
            MayAccess(
                'site:party1.mahiru.example.org:site1',
                'asset_collection:party1.mahiru.example.org:collection.PII1'),
            ResultOfDataIn(
                'asset_collection:party1.mahiru.example.org:collection.PII1',
                '*', '*',
                'asset_collection:party1.mahiru.example.org:collection.PII1'),
            ResultOfDataIn(
                'asset_collection:party1.mahiru.example.org:collection.PII1',
                'asset:party3.mahiru.example.org:software.anonymise'
                ':party3.mahiru.example.org:site3', 'y',
                'asset_collection:party1.mahiru.example.org'
                ':collection.ScienceOnly1'),
            ResultOfDataIn(
                'asset_collection:party1.mahiru.example.org:collection.PII1',
                'asset:party3.mahiru.example.org:software.aggregate'
                ':party3.mahiru.example.org:site3', 'y',
                'asset_collection:party3.mahiru.example.org'
                ':collection.Public'),
            ResultOfDataIn(
                'asset_collection:party1.mahiru.example.org'
                ':collection.ScienceOnly1', '*', '*',
                'asset_collection:party1.mahiru.example.org'
                ':collection.ScienceOnly1'),
            InAssetCollection(
                'asset_collection:party1.mahiru.example.org'
                ':collection.ScienceOnly1',
                'asset_collection:party3.mahiru.example.org'
                ':collection.ScienceOnly'),
            ]

    scenario['rules-party2'] = [
            InAssetCollection(
                'asset:party2.mahiru.example.org:dataset.pii2'
                ':party2.mahiru.example.org:site2',
                'asset_collection:party2.mahiru.example.org:collection.PII2'),
            MayAccess(
                'site:party2.mahiru.example.org:site2',
                'asset_collection:party2.mahiru.example.org:collection.PII2'),
            MayAccess(
                'site:party1.mahiru.example.org:site1',
                'asset_collection:party2.mahiru.example.org:collection.PII2'),
            ResultOfDataIn(
                'asset_collection:party2.mahiru.example.org:collection.PII2',
                '*', '*',
                'asset_collection:party2.mahiru.example.org:collection.PII2'),
            ResultOfDataIn(
                'asset_collection:party2.mahiru.example.org:collection.PII2',
                'asset:party3.mahiru.example.org:software.anonymise:'
                'party3.mahiru.example.org:site3', 'y',
                'asset_collection:party2.mahiru.example.org'
                ':collection.ScienceOnly2'),
            ResultOfDataIn(
                'asset_collection:party2.mahiru.example.org'
                ':collection.ScienceOnly2', '*', '*',
                'asset_collection:party2.mahiru.example.org'
                ':collection.ScienceOnly2'),
            InAssetCollection(
                'asset_collection:party2.mahiru.example.org'
                ':collection.ScienceOnly2',
                'asset_collection:party3.mahiru.example.org'
                ':collection.ScienceOnly'),
            ]

    scenario['rules-party3'] = [
            InAssetCollection(
                'asset:party3.mahiru.example.org:software.anonymise'
                ':party3.mahiru.example.org:site3',
                'asset_collection:party3.mahiru.example.org'
                ':collection.PublicSoftware'),
            InAssetCollection(
                'asset:party3.mahiru.example.org:software.aggregate'
                ':party3.mahiru.example.org:site3',
                'asset_collection:party3.mahiru.example.org'
                ':collection.PublicSoftware'),
            InAssetCollection(
                'asset:party3.mahiru.example.org:software.combine'
                ':party3.mahiru.example.org:site3',
                'asset_collection:party3.mahiru.example.org'
                ':collection.PublicSoftware'),
            MayAccess(
                '*',
                'asset_collection:party3.mahiru.example.org'
                ':collection.PublicSoftware'),
            ResultOfDataIn(
                'asset_collection:party3.mahiru.example.org:collection.Public',
                '*', '*',
                'asset_collection:party3.mahiru.example.org:collection.Public'
                ),

            ResultOfComputeIn(
                '*',
                'asset:party3.mahiru.example.org:software.anonymise'
                ':party3.mahiru.example.org:site3',
                'y',
                'asset_collection:party3.mahiru.example.org:collection.Public'
                ),

            ResultOfComputeIn(
                '*',
                'asset:party3.mahiru.example.org:software.aggregate'
                ':party3.mahiru.example.org:site3',
                'y',
                'asset_collection:party3.mahiru.example.org:collection.Public'
                ),

            ResultOfComputeIn(
                '*',
                'asset:party3.mahiru.example.org:software.combine'
                ':party3.mahiru.example.org:site3',
                'y',
                'asset_collection:party3.mahiru.example.org:collection.Public'
                ),

            MayAccess(
                'site:party3.mahiru.example.org:site3',
                'asset_collection:party3.mahiru.example.org'
                ':collection.ScienceOnly'),
            MayUse(
                'party:party2.mahiru.example.org:party2',
                'asset_collection:party3.mahiru.example.org'
                ':collection.ScienceOnly',
                'Only for non-commercial scientific purposes'),

            MayAccess(
                'site:party1.mahiru.example.org:site1',
                'asset_collection:party3.mahiru.example.org:collection.Public'
                ),
            MayUse(
                'party:party1.mahiru.example.org:party1',
                'asset_collection:party3.mahiru.example.org:collection.Public',
                'For any purpose'),

            MayAccess(
                'site:party2.mahiru.example.org:site2',
                'asset_collection:party3.mahiru.example.org:collection.Public'
                ),
            MayUse(
                'party:party2.mahiru.example.org:party2',
                'asset_collection:party3.mahiru.example.org:collection.Public',
                'For any purpose'),

            MayAccess(
                'site:party3.mahiru.example.org:site3',
                'asset_collection:party3.mahiru.example.org:collection.Public'
                ),
            MayUse(
                'party:party3.mahiru.example.org:party3',
                'asset_collection:party3.mahiru.example.org:collection.Public',
                'For any purpose'),
            ]

    scenario['sites'] = {
            'site:party1.mahiru.example.org:site1': {
                'owner': 'party:party1.mahiru.example.org:party1',
                'namespace': 'party1.mahiru.example.org',
                'https_certificate': site1_https_certificate,
                'assets': [
                    DataAsset(
                        'asset:party1.mahiru.example.org:dataset.pii1'
                        ':party1.mahiru.example.org:site1', 42)],
                'rules': scenario['rules-party1']},
            'site:party2.mahiru.example.org:site2': {
                'owner': 'party:party2.mahiru.example.org:party2',
                'namespace': 'party2.mahiru.example.org',
                'https_certificate': site2_https_certificate,
                'assets': [
                    DataAsset(
                        'asset:party2.mahiru.example.org:dataset.pii2'
                        ':party2.mahiru.example.org:site2', 3)],
                'rules': scenario['rules-party2']},
            'site:party3.mahiru.example.org:site3': {
                'owner': 'party:party3.mahiru.example.org:party3',
                'namespace': 'party3.mahiru.example.org',
                'https_certificate': site3_https_certificate,
                'assets': [
                    ComputeAsset(
                        'asset:party3.mahiru.example.org:software.combine'
                        ':party3.mahiru.example.org:site3',
                        None, None),
                    ComputeAsset(
                        'asset:party3.mahiru.example.org:software.anonymise'
                        ':party3.mahiru.example.org:site3',
                        None, None),
                    ComputeAsset(
                        'asset:party3.mahiru.example.org:software.aggregate'
                        ':party3.mahiru.example.org:site3',
                        None, None)],
                'rules': scenario['rules-party3']}}

    workflow = Workflow(
            ['x1', 'x2'], {'result': 'aggregate.y'}, [
                WorkflowStep(
                    name='combine',
                    inputs={'x1': 'x1', 'x2': 'x2'},
                    outputs={'y': None},
                    compute_asset_id=(
                        'asset:party3.mahiru.example.org:software.combine'
                        ':party3.mahiru.example.org:site3')),
                WorkflowStep(
                    name='anonymise',
                    inputs={'x1': 'combine.y'},
                    outputs={'y': None},
                    compute_asset_id=(
                        'asset:party3.mahiru.example.org:software.anonymise'
                        ':party3.mahiru.example.org:site3')),
                WorkflowStep(
                    name='aggregate',
                    inputs={'x1': 'anonymise.y'},
                    outputs={'y': None},
                    compute_asset_id=(
                        'asset:party3.mahiru.example.org:software.aggregate'
                        ':party3.mahiru.example.org:site3'))
            ]
    )

    inputs = {
            'x1': 'asset:party1.mahiru.example.org:dataset.pii1'
            ':party1.mahiru.example.org:site1',
            'x2': 'asset:party2.mahiru.example.org:dataset.pii2'
            ':party2.mahiru.example.org:site2'}

    scenario['job'] = Job(
            Identifier('party:party2.mahiru.example.org:party2'),
            workflow, inputs)
    scenario['user_site'] = 'site:party2.mahiru.example.org:site2'

    output = run_scenario(scenario, registry_client, registration_client)
    assert output['result'].data == 12.5


def test_saas_with_data(
        registry_server, registry_client, registration_client,
        party1_main_certificate, party1_user_ca_certificate,
        party1_main_key, site1_https_certificate,
        party2_main_certificate, party2_user_ca_certificate,
        party2_main_key, site2_https_certificate):

    scenario = dict()     # type: Dict[str, Any]

    scenario['parties'] = {
            'party:party1.mahiru.example.org:party1': {
                'namespace': 'party1.mahiru.example.org',
                'main_certificate': party1_main_certificate,
                'main_key': party1_main_key,
                'user_ca_certificate': party1_user_ca_certificate,
                'user_certificates': []},
            'party:party2.mahiru.example.org:party2': {
                'namespace': 'party2.mahiru.example.org',
                'main_certificate': party2_main_certificate,
                'main_key': party2_main_key,
                'user_ca_certificate': party2_user_ca_certificate,
                'user_certificates': []}
            }

    scenario['rules-party1'] = [
            MayAccess(
                'site:party1.mahiru.example.org:site1',
                'asset:party1.mahiru.example.org:dataset.data1'
                ':party1.mahiru.example.org:site1'),
            MayAccess(
                'site:party2.mahiru.example.org:site2',
                'asset:party1.mahiru.example.org:dataset.data1'
                ':party1.mahiru.example.org:site1'),
            ResultOfDataIn(
                'asset:party1.mahiru.example.org:dataset.data1'
                ':party1.mahiru.example.org:site1',
                'asset:party2.mahiru.example.org:software.addition'
                ':party2.mahiru.example.org:site2',
                'y',
                'asset_collection:party1.mahiru.example.org:collection.result1'
                ),
            MayAccess(
                'site:party1.mahiru.example.org:site1',
                'asset_collection:party1.mahiru.example.org:collection.result1'
                ),
            MayAccess(
                'site:party2.mahiru.example.org:site2',
                'asset_collection:party1.mahiru.example.org:collection.result1'
                ),
            MayUse(
                'party:party1.mahiru.example.org:party1',
                'asset_collection:party1.mahiru.example.org'
                ':collection.result1',
                'For any use'),
            ]

    scenario['rules-party2'] = [
            MayAccess(
                'site:party2.mahiru.example.org:site2',
                'asset:party2.mahiru.example.org:dataset.data2'
                ':party2.mahiru.example.org:site2'),
            MayAccess(
                'site:party2.mahiru.example.org:site2',
                'asset:party2.mahiru.example.org:software.addition'
                ':party2.mahiru.example.org:site2'),
            ResultOfDataIn(
                'asset:party2.mahiru.example.org:dataset.data2'
                ':party2.mahiru.example.org:site2',
                'asset:party2.mahiru.example.org:software.addition'
                ':party2.mahiru.example.org:site2', 'y',
                'asset_collection:party2.mahiru.example.org'
                ':collection.result2'),
            ResultOfComputeIn(
                'asset:party2.mahiru.example.org:dataset.data2'
                ':party2.mahiru.example.org:site2',
                'asset:party2.mahiru.example.org:software.addition'
                ':party2.mahiru.example.org:site2', '*',
                'asset_collection:party2.mahiru.example.org:collection.result2'
                ),
            ResultOfComputeIn(
                'asset:party1.mahiru.example.org:dataset.data1'
                ':party1.mahiru.example.org:site1',
                'asset:party2.mahiru.example.org:software.addition'
                ':party2.mahiru.example.org:site2', 'y',
                'asset_collection:party2.mahiru.example.org:collection.result2'
                ),
            MayAccess(
                'site:party1.mahiru.example.org:site1',
                'asset_collection:party2.mahiru.example.org:collection.result2'
                ),
            MayAccess(
                'site:party2.mahiru.example.org:site2',
                'asset_collection:party2.mahiru.example.org:collection.result2'
                ),
            MayAccess(
                'site:party2.mahiru.example.org:site2',
                'asset:party2.mahiru.example.org:software.addition'
                ':party2.mahiru.example.org:site2'),
            MayUse(
                'party:party1.mahiru.example.org:party1',
                'asset_collection:party2.mahiru.example.org'
                ':collection.result2',
                'For any use'),
            ]

    scenario['sites'] = {
            'site:party1.mahiru.example.org:site1': {
                'owner': 'party:party1.mahiru.example.org:party1',
                'namespace': 'party1.mahiru.example.org',
                'https_certificate': site1_https_certificate,
                'assets': [
                    DataAsset(
                        'asset:party1.mahiru.example.org:dataset.data1:'
                        'party1.mahiru.example.org:site1', 42)],
                'rules': scenario['rules-party1']},
            'site:party2.mahiru.example.org:site2': {
                'owner': 'party:party2.mahiru.example.org:party2',
                'namespace': 'party2.mahiru.example.org',
                'https_certificate': site2_https_certificate,
                'assets': [
                    DataAsset(
                        'asset:party2.mahiru.example.org:dataset.data2'
                        ':party2.mahiru.example.org:site2', 3),
                    ComputeAsset(
                         'asset:party2.mahiru.example.org:software.addition'
                         ':party2.mahiru.example.org:site2',
                         None, None)],
                'rules': scenario['rules-party2']}}

    workflow = Workflow(inputs=['x1', 'x2'],
                        outputs={'y': 'addstep.y'},
                        steps=[
                WorkflowStep(
                    name='addstep',
                    inputs={'x1': 'x1', 'x2': 'x2'},
                    outputs={'y': None},
                    compute_asset_id=(
                        'asset:party2.mahiru.example.org:software.addition:'
                        'party2.mahiru.example.org:site2'))
                ])

    inputs = {
            'x1': (
                'asset:party1.mahiru.example.org:dataset.data1'
                ':party1.mahiru.example.org:site1'),
            'x2': (
                'asset:party2.mahiru.example.org:dataset.data2'
                ':party2.mahiru.example.org:site2')}

    scenario['job'] = Job(
            Identifier('party:party1.mahiru.example.org:party1'),
            workflow, inputs)
    scenario['user_site'] = 'site:party1.mahiru.example.org:site1'

    output = run_scenario(scenario, registry_client, registration_client)
    assert output['y'].data == 45
