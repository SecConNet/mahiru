import gzip
import logging
from unittest.mock import patch

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric.rsa import (
        generate_private_key, RSAPrivateKey)
import docker
import pytest
import time

from proof_of_concept.components.ddm_site import Site
from proof_of_concept.components.registry_client import RegistryClient
from proof_of_concept.definitions.assets import ComputeAsset, DataAsset
from proof_of_concept.definitions.identifier import Identifier
from proof_of_concept.definitions.registry import (
        PartyDescription, SiteDescription)
from proof_of_concept.definitions.workflows import Job, Workflow, WorkflowStep
from proof_of_concept.policy.rules import (
        MayAccess, ResultOfComputeIn, ResultOfDataIn)
from proof_of_concept.rest.ddm_site import SiteRestApi, SiteServer
from proof_of_concept.rest.internal_client import InternalSiteRestClient


logger = logging.getLogger(__file__)


@pytest.fixture
def dcli():
    """Docker client object."""
    return docker.from_env()


@pytest.fixture
def docker_dir(test_dir):
    """Path to the tests/docker directory."""
    return test_dir / 'docker'


def _build_image(dcli, docker_dir, name):
    img, _ = dcli.images.build(
            path=str(docker_dir),
            tag=f'mahiru-test/{name}',
            dockerfile=str(docker_dir / f'{name}.Dockerfile'),
            rm=True)
    return img


def _make_tarfile(dcli, docker_dir, image, name):
    tar_path = docker_dir / f'{name}.tar.gz'
    with gzip.open(str(tar_path), 'wb', compresslevel=1) as f:
        for chunk in image.save():
            f.write(chunk)
    return tar_path


def _build_data_images(dcli, docker_dir):
    base_image = None
    input_image = None
    base_file = None
    input_file = None
    try:
        base_image = _build_image(dcli, docker_dir, 'data-asset-base')
        input_image = _build_image(dcli, docker_dir, 'data-asset-input')
        base_file = _make_tarfile(
                dcli, docker_dir, base_image, 'data-asset-base')
        input_file = _make_tarfile(
                dcli, docker_dir, input_image, 'data-asset-input')
        return base_file, input_file
    except Exception:
        if input_file:
            input_file.unlink(missing_ok=True)
        if base_file:
            base_file.unlink(missing_ok=True)
        raise
    finally:
        if input_image:
            dcli.images.remove('mahiru-test/data-asset-input')
        if base_image:
            dcli.images.remove('mahiru-test/data-asset-base')


def _build_compute_image(dcli, docker_dir):
    base_image = None
    input_image = None
    input_file = None
    try:
        base_image = _build_image(dcli, docker_dir, 'compute-asset-base')
        input_image = _build_image(dcli, docker_dir, 'compute-asset')
        input_file = _make_tarfile(
                dcli, docker_dir, input_image, 'compute-asset')
        return input_file
    except Exception:
        if input_file:
            input_file.unlink(missing_ok=True)
        raise
    finally:
        if input_image:
            dcli.images.remove('mahiru-test/compute-asset')
        if base_image:
            dcli.images.remove('mahiru-test/compute-asset-base')


@pytest.fixture
def data_asset_tars(dcli, docker_dir):
    base_file, input_file = _build_data_images(dcli, docker_dir)
    yield base_file, input_file
    input_file.unlink()
    base_file.unlink()


@pytest.fixture
def compute_asset_tar(dcli, docker_dir):
    compute_file = _build_compute_image(dcli, docker_dir)
    yield compute_file
    compute_file.unlink()


@patch(
        'proof_of_concept.components.domain_administrator.OUTPUT_ASSET_ID',
        Identifier('asset:ns:output_base:ns:test_site'))
def test_container_step(
        registry_server, data_asset_tars, compute_asset_tar, caplog):

    caplog.set_level(logging.DEBUG)

    registry_client = RegistryClient()

    # create party
    party_key = generate_private_key(
            public_exponent=65537, key_size=2048, backend=default_backend())

    registry_client.register_party(
            PartyDescription('party:ns:test_party', party_key.public_key()))

    # create assets
    data_asset_output_tar, data_asset_input_tar = data_asset_tars
    assets = [
            DataAsset(
                'asset:ns:dataset1:ns:test_site', None,
                str(data_asset_input_tar)),
            ComputeAsset(
                'asset:ns:compute1:ns:test_site', None,
                str(compute_asset_tar)),
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
            ResultOfComputeIn(
                '*', 'asset:ns:compute1:ns:test_site',
                'asset_collection:ns:results1'),
            MayAccess('site:ns:test_site', 'asset_collection:ns:results1')]

    for rule in rules:
        rule.sign(party_key)

    # create site
    site = Site(
            'test_site', 'party:ns:test_party', 'ns',
            [], [], registry_client)

    site_server = SiteServer(SiteRestApi(
        site.policy_store, site.store, site.runner))

    internal_client = InternalSiteRestClient(site_server.internal_endpoint)
    for asset in assets:
        internal_client.store_asset(asset)

    for rule in rules:
        internal_client.add_rule(rule)

    registry_client.register_site(
        SiteDescription(
                site.id, site.owner, site.administrator,
                site_server.external_endpoint,
                True, True, site.namespace))

    # create single-step workflow
    workflow = Workflow(
            ['input'], {'result': 'compute.output0'}, [
                WorkflowStep(
                    name='compute',
                    inputs={'input0': 'input'},
                    outputs=['output0'],
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
        registry_client.deregister_site(site.id)
        registry_client.deregister_party('party:ns:test_party')
        site.close()
