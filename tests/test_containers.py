import gzip
import logging
from pathlib import Path
from unittest.mock import patch

from cryptography import x509
from cryptography.hazmat.primitives.serialization import load_pem_private_key
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
        MayAccess, MayUse, ResultOfComputeIn, ResultOfDataIn)
from mahiru.rest.ddm_site import SiteRestApi, SiteServer
from mahiru.rest.internal_client import InternalSiteRestClient


logger = logging.getLogger(__file__)


BUILD_DIR = Path(__file__).parents[1] / 'build'
IMAGE_DIR = BUILD_DIR / 'images'
CERTS_DIR = BUILD_DIR / 'certs' / 'site1' / 'certs'
PRIVATE_DIR = BUILD_DIR / 'certs' / 'site1' / 'private'


def get_tar_file(name):
    tar_file = IMAGE_DIR / name
    if not tar_file.exists():
        pytest.skip(
                f"Image {name} not available. Run 'make assets docker_tars'"
                " to build the images needed for this test.")
    return tar_file


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
        data_asset_tars, compute_asset_tar, network_settings):

    # load certificates
    cert_file = CERTS_DIR / 'party1_main_cert.pem'
    with cert_file.open('rb') as f:
        main_cert = x509.load_pem_x509_certificate(f.read())

    cert_file = CERTS_DIR / 'party1_user_ca_cert.pem'
    with cert_file.open('rb') as f:
        user_ca_cert = x509.load_pem_x509_certificate(f.read())

    # create party
    party = Identifier('party:party1.mahiru.example.org:party1')
    registration_client.register_party(
            PartyDescription(
                party, 'party1.mahiru.example.org', main_cert, user_ca_cert,
                []))

    # create assets
    data_asset_output_tar, data_asset_input_tar = data_asset_tars
    assets = [
            DataAsset(
                'asset:party1.mahiru.example.org:dataset1'
                ':party1.mahiru.example.org:site1', None,
                str(data_asset_input_tar)),
            ComputeAsset(
                'asset:party1.mahiru.example.org:compute1'
                ':party1.mahiru.example.org:site1', None,
                str(compute_asset_tar),
                ComputeMetadata(
                    {'output0':
                        'asset:party1.mahiru.example.org:output_base'
                        ':party1.mahiru.example.org:site1'})),
            DataAsset(
                'asset:party1.mahiru.example.org:output_base'
                ':party1.mahiru.example.org:site1', None,
                str(data_asset_output_tar))]

    # create rules
    rules = [
            MayAccess(
                'site:party1.mahiru.example.org:site1',
                'asset:party1.mahiru.example.org:dataset1'
                ':party1.mahiru.example.org:site1'),
            MayAccess(
                'site:party1.mahiru.example.org:site1',
                'asset:party1.mahiru.example.org:compute1'
                ':party1.mahiru.example.org:site1'),
            MayAccess(
                'site:party1.mahiru.example.org:site1',
                'asset:party1.mahiru.example.org:output_base'
                ':party1.mahiru.example.org:site1'),
            ResultOfDataIn(
                'asset:party1.mahiru.example.org:dataset1'
                ':party1.mahiru.example.org:site1', '*', 'output0',
                'asset_collection:party1.mahiru.example.org:results1'),
            ResultOfDataIn(
                'asset:party1.mahiru.example.org:output_base'
                ':party1.mahiru.example.org:site1', '*', '*',
                'asset_collection:party1.mahiru.example.org:results1'),
            ResultOfComputeIn(
                '*', 'asset:party1.mahiru.example.org:compute1'
                ':party1.mahiru.example.org:site1', '*',
                'asset_collection:party1.mahiru.example.org:public'),
            MayAccess(
                'site:party1.mahiru.example.org:site1',
                'asset_collection:party1.mahiru.example.org:results1'),
            MayUse(
                'party:party1.mahiru.example.org:party1',
                'asset_collection:party1.mahiru.example.org:results1', ''),
            MayAccess(
                '*', 'asset_collection:party1.mahiru.example.org:public'),
            MayUse(
                '*', 'asset_collection:party1.mahiru.example.org:public',
                'For any use')]

    key_file = PRIVATE_DIR / 'party1_main_key.pem'
    with key_file.open('rb') as f:
        main_key = load_pem_private_key(f.read(), None)

    for rule in rules:
        rule.sign(main_key)

    # create site
    cert_file = CERTS_DIR / 'site1_https_cert.pem'
    with cert_file.open('rb') as f:
        https_cert = x509.load_pem_x509_certificate(f.read())

    config = SiteConfiguration(
            'site:party1.mahiru.example.org:site1',
            'party1.mahiru.example.org', party, network_settings, '')
    site = Site(config, [], [], registry_client)

    site_server = SiteServer(SiteRestApi(
        site.policy_store, site.store, site.runner, site.orchestrator))

    # wait for it to come up
    requests.get(site_server.internal_endpoint, timeout=(600.0, 1.0))

    # initialise site
    internal_client = InternalSiteRestClient(
            site.owner, site.id, site_server.internal_endpoint)
    for asset in assets:
        internal_client.store_asset(asset)

    for rule in rules:
        internal_client.add_rule(rule)

    registration_client.register_site(
        SiteDescription(
                site.id, site.owner, site.administrator,
                site_server.external_endpoint, https_cert,
                True, True, True))

    # create single-step workflow
    workflow = Workflow(
            ['input'], {'result': 'compute.output0'}, [
                WorkflowStep(
                    name='compute',
                    inputs={'input0': 'input'},
                    outputs={
                        'output0':
                            'asset:party1.mahiru.example.org:output_base'
                            ':party1.mahiru.example.org:site1'},
                    compute_asset_id=(
                        'asset:party1.mahiru.example.org:compute1'
                        ':party1.mahiru.example.org:site1'))
            ]
    )

    inputs = {
            'input':
            'asset:party1.mahiru.example.org:dataset1'
            ':party1.mahiru.example.org:site1'}

    # run workflow
    try:
        result = site.run_job(Job(party, workflow, inputs))
    finally:
        site_server.close()
        registration_client.deregister_site(site.id)
        registration_client.deregister_party(party)
        site.close()


def test_container_step(
        registry_server, registry_client, registration_client,
        data_asset_tars, compute_asset_tar, caplog):

    caplog.set_level(logging.DEBUG)
    run_container_step(
            registry_server, registry_client, registration_client,
            data_asset_tars, compute_asset_tar, NetworkSettings())


def test_container_connections(
        registry_server, registry_client, registration_client,
        data_asset_tars, compute_asset_tar, caplog):

    caplog.set_level(logging.DEBUG)

    network_settings = NetworkSettings(True, '127.0.0.1', [10000, 11000])
    run_container_step(
            registry_server, registry_client, registration_client,
            data_asset_tars, compute_asset_tar, network_settings)

    assert (
            'mahiru.components.domain_administrator', logging.DEBUG,
            'Nets: {}') not in caplog.record_tuples
