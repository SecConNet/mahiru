import logging
from textwrap import indent
import time
from typing import Any, Dict

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric.rsa import (
        generate_private_key, RSAPrivateKey)
import requests

from mahiru.components.ddm_site import Site
from mahiru.components.registry_client import RegistryClient
from mahiru.components.settings import AssetConnections, Settings
from mahiru.definitions.assets import ComputeAsset, DataAsset
from mahiru.definitions.registry import PartyDescription, SiteDescription
from mahiru.definitions.workflows import Job, WorkflowStep, Workflow
from mahiru.policy.rules import (
    InAssetCollection, MayAccess, ResultOfDataIn,
    ResultOfComputeIn)
from mahiru.rest.ddm_site import SiteRestApi, SiteServer
from mahiru.rest.internal_client import InternalSiteRestClient
from mahiru.rest.registry_client import RegistrationRestClient


logger = logging.getLogger(__file__)


def create_parties(
        site_descriptions: Dict[str, Any]
        ) -> Dict[str, RSAPrivateKey]:
    """Creates parties with private keys."""
    return {
            desc['owner']: generate_private_key(
                public_exponent=65537,
                key_size=2048,
                backend=default_backend())
            for desc in site_descriptions.values()}


def register_parties(
        registration_client: RegistrationRestClient,
        parties: Dict[str, RSAPrivateKey]
        ) -> None:
    """Register parties with their public keys."""
    for party_id, private_key in parties.items():
        registration_client.register_party(
                PartyDescription(party_id, private_key.public_key()))


def sign_rules(
        site_descriptions: Dict[str, Any], parties: Dict[str, RSAPrivateKey]
        ) -> None:
    """Update site descriptions by signing rules."""
    for desc in site_descriptions.values():
        private_key = parties[desc['owner']]
        for rule in desc['rules']:
            rule.sign(private_key)


def create_sites(
        registry_client: RegistryClient,
        site_descriptions: Dict[str, Any]
        ) -> Dict[str, Site]:
    """Creates sites for the scenario."""
    return {
            site_name: Site(
                Settings(
                    site_name, desc['namespace'], desc['owner'],
                    AssetConnections, ''),
                [], [], registry_client)
            for site_name, desc in site_descriptions.items()}


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
                sites[site_name].id, server.internal_endpoint)
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
        registration_client: RegistrationRestClient, sites: Dict[str, Site],
        servers: Dict[str, SiteServer]) -> None:
    """Register sites with the registry."""
    for site_name, site in sites.items():
        registration_client.register_site(
                SiteDescription(
                    site.id, site.owner, site.administrator,
                    servers[site_name].external_endpoint,
                    True, True, site.namespace))


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
        parties: Dict[str, RSAPrivateKey]
        ) -> None:
    """Deregisters parties from the registry."""
    for party_id in parties:
        registration_client.deregister_party(party_id)


def run_scenario(
        scenario: Dict[str, Any], registry_client: RegistryClient,
        registration_client: RegistrationRestClient
        ) -> Dict[str, Any]:
    logger.info('Running test scenario on behalf of {}'.format(
        scenario['user_site']))
    logger.info('Job:\n{}'.format(indent(str(scenario["job"]), " "*4)))

    parties = create_parties(scenario['sites'])
    register_parties(registration_client, parties)

    sign_rules(scenario['sites'], parties)
    sites = create_sites(registry_client, scenario['sites'])
    servers = create_servers(sites)
    clients = create_clients(servers, sites)
    upload_assets(scenario['sites'], clients)
    add_rules(scenario['sites'], clients)
    register_sites(registration_client, sites, servers)

    client = clients[scenario['user_site']]
    job_id = client.submit_job(scenario['job'])
    while not client.is_job_done(job_id):
        time.sleep(0.1)
    result = client.get_job_result(job_id)

    stop_servers(servers)
    deregister_sites(registration_client, sites)
    deregister_parties(registration_client, parties)

    logger.info(f'Result: {result.outputs}')
    return result.outputs


def test_pii(registry_server, registry_client, registration_client):
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
                    outputs={'y': None},
                    compute_asset_id=(
                        'asset:ddm_ns:software.combine:ddm_ns:site3')),
                WorkflowStep(
                    name='anonymise',
                    inputs={'x1': 'combine.y'},
                    outputs={'y': None},
                    compute_asset_id=(
                        'asset:ddm_ns:software.anonymise:ddm_ns:site3')),
                WorkflowStep(
                    name='aggregate',
                    inputs={'x1': 'anonymise.y'},
                    outputs={'y': None},
                    compute_asset_id=(
                        'asset:ddm_ns:software.aggregate:ddm_ns:site3'))
            ]
    )

    inputs = {
            'x1': 'asset:party1_ns:dataset.pii1:party1_ns:site1',
            'x2': 'asset:party2_ns:dataset.pii2:party2_ns:site2'}

    scenario['job'] = Job(workflow, inputs)
    scenario['user_site'] = 'site2'

    output = run_scenario(scenario, registry_client, registration_client)
    assert output['result'].data == 12.5


def test_saas_with_data(registry_server, registry_client, registration_client):
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
                    outputs={'y': None},
                    compute_asset_id=(
                        'asset:party2_ns:software.addition:party2_ns:site2'))
                ])

    inputs = {
            'x1': 'asset:party1_ns:dataset.data1:party1_ns:site1',
            'x2': 'asset:party2_ns:dataset.data2:party2_ns:site2'}

    scenario['job'] = Job(workflow, inputs)
    scenario['user_site'] = 'site1'

    output = run_scenario(scenario, registry_client, registration_client)
    assert output['y'].data == 45
