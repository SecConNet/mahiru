import gzip
import logging
from pathlib import Path
from unittest.mock import patch

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric.rsa import (
        generate_private_key, RSAPrivateKey)
import pytest
import requests
import time

from mahiru.components.ddm_site import Site
from mahiru.components.settings import NetworkSettings, SiteConfiguration
from mahiru.definitions.assets import (
        ComputeAsset, ComputeMetadata, DataAsset)
from mahiru.definitions.identifier import Identifier
from mahiru.definitions.registry import PartyDescription, SiteDescription
from mahiru.definitions.workflows import Job, Workflow, WorkflowStep
from mahiru.policy.rules import (
        MayAccess, ResultOfComputeIn, ResultOfDataIn)
from mahiru.rest.ddm_site import SiteRestApi, SiteServer
from mahiru.rest.internal_client import InternalSiteRestClient


logger = logging.getLogger(__file__)


def get_tar_file(name):
    asset_dir = Path(__file__).parents[1] / 'build'
    tar_file = asset_dir / name
    if not tar_file.exists():
        pytest.skip(
                f"Image {name} not available. Run 'make assets docker_tars'"
                " to build the images needed for this test.")
    return tar_file


@pytest.fixture
def pilot_tar():
    return get_tar_file('pilot.tar.gz')


@pytest.fixture
def data_asset_tars():
    base_file = get_tar_file('data-asset-base.tar.gz')
    input_file = get_tar_file('data-asset-input.tar.gz')
    return base_file, input_file


@pytest.fixture
def compute_asset_tar():
    return get_tar_file('compute-asset.tar.gz')


def run_container_step(
        registry_server, registry_client, registration_client,
        pilot_tar, data_asset_tars, compute_asset_tar, network_settings):

    # create party
    party_key = generate_private_key(
            public_exponent=65537, key_size=2048, backend=default_backend())

    registration_client.register_party(
            PartyDescription(
                'party:ns:test_party', 'ns', party_key.public_key()))

    # create assets
    data_asset_output_tar, data_asset_input_tar = data_asset_tars
    assets = [
            DataAsset(
                'asset:ns:dataset1:ns:test_site', None,
                str(data_asset_input_tar)),
            ComputeAsset(
                'asset:ns:compute1:ns:test_site', None,
                str(compute_asset_tar),
                ComputeMetadata(
                    {'output0': 'asset:ns:output_base:ns:test_site'})),
            DataAsset(
                'asset:ns:output_base:ns:test_site', None,
                str(data_asset_output_tar))]

    # create rules
    rules = [
            MayAccess('site:ns:test_site', 'asset:ns:dataset1:ns:test_site'),
            MayAccess('site:ns:test_site', 'asset:ns:compute1:ns:test_site'),
            MayAccess(
                'site:ns:test_site', 'asset:ns:output_base:ns:test_site'),
            ResultOfDataIn(
                'asset:ns:dataset1:ns:test_site', '*',
                'asset_collection:ns:results1'),
            ResultOfDataIn(
                'asset:ns:output_base:ns:test_site', '*',
                'asset_collection:ns:results1'),
            ResultOfComputeIn(
                '*', 'asset:ns:compute1:ns:test_site',
                'asset_collection:ns:public'),
            MayAccess('site:ns:test_site', 'asset_collection:ns:results1'),
            MayAccess('*', 'asset_collection:ns:public')]

    for rule in rules:
        rule.sign(party_key)

    # create site
    config = SiteConfiguration(
            'test_site', 'ns', 'party:ns:test_party', network_settings, '')
    site = Site(config, [], [], registry_client)

    site_server = SiteServer(SiteRestApi(
        site.policy_store, site.store, site.runner, site.orchestrator))

    # wait for it to come up
    requests.get(site_server.internal_endpoint, timeout=(600.0, 1.0))

    # initialise site
    internal_client = InternalSiteRestClient(
            site.id, site_server.internal_endpoint)
    for asset in assets:
        internal_client.store_asset(asset)

    for rule in rules:
        internal_client.add_rule(rule)

    registration_client.register_site(
        SiteDescription(
                site.id, site.owner, site.administrator,
                site_server.external_endpoint,
                True, True, True))

    # create single-step workflow
    workflow = Workflow(
            ['input'], {'result': 'compute.output0'}, [
                WorkflowStep(
                    name='compute',
                    inputs={'input0': 'input'},
                    outputs={'output0': 'asset:ns:output_base:ns:test_site'},
                    compute_asset_id=(
                        'asset:ns:compute1:ns:test_site'))
            ]
    )

    inputs = {'input': 'asset:ns:dataset1:ns:test_site'}

    # run workflow
    try:
        result = site.run_job(Job(workflow, inputs))
    finally:
        site_server.close()
        registration_client.deregister_site(site.id)
        registration_client.deregister_party('party:ns:test_party')
        site.close()


def test_container_step(
        registry_server, registry_client, registration_client,
        pilot_tar, data_asset_tars, compute_asset_tar, caplog):

    caplog.set_level(logging.DEBUG)
    run_container_step(
            registry_server, registry_client, registration_client, pilot_tar,
            data_asset_tars, compute_asset_tar, NetworkSettings())


def test_container_connections(
        registry_server, registry_client, registration_client,
        pilot_tar, data_asset_tars, compute_asset_tar, caplog):

    caplog.set_level(logging.DEBUG)

    network_settings = NetworkSettings(True, '127.0.0.1', [10000, 11000])
    run_container_step(
            registry_server, registry_client, registration_client, pilot_tar,
            data_asset_tars, compute_asset_tar, network_settings)

    assert (
            'mahiru.components.domain_administrator', logging.DEBUG,
            'Nets: {}') not in caplog.record_tuples
