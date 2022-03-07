#!/usr/bin/env python3

from pathlib import Path

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric.rsa import (
        generate_private_key, RSAPrivateKey, RSAPublicKey)
from cryptography.hazmat.primitives.serialization import (
    Encoding, PublicFormat)
import requests

from mahiru.definitions.assets import ComputeAsset, ComputeMetadata, DataAsset
from mahiru.definitions.registry import PartyDescription, SiteDescription
from mahiru.policy.rules import (
        MayAccess, ResultOfComputeIn, ResultOfDataIn)
from mahiru.rest.registry_client import RegistrationRestClient
from mahiru.rest.internal_client import InternalSiteRestClient


ASSET_DIR = Path.home() / 'mahiru' / 'assets'


def register(public_key: RSAPublicKey) -> None:
    """Registers the current party and site with the registry."""
    party_id = 'party:party1_ns:party1'
    namespace = 'party1_ns'
    party_desc = PartyDescription(party_id, namespace, public_key)

    site_id = 'site:party1_ns:site1'
    endpoint = 'http://site1'
    site_desc = SiteDescription(
            site_id, party_id, party_id, endpoint, True, True, True)

    client = RegistrationRestClient("http://registry")

    # Remove stale registrations, if any
    try:
        client.deregister_site(site_id)
    except KeyError:
        pass
    try:
        client.deregister_party(party_id)
    except KeyError:
        pass

    client.register_party(party_desc)
    client.register_site(site_desc)


def add_initial_assets(client: InternalSiteRestClient) -> None:
    output_base = DataAsset(
            'asset:party1_ns:da.data.output_base:party1_ns:site1', None,
            f'{ASSET_DIR}/data-asset-base.tar.gz')

    script_metadata = ComputeMetadata({
            'output0': 'asset:party1_ns:da.data.output_base:party1_ns:site1'})

    script = ComputeAsset(
            'asset:party1_ns:da.software.script1:party1_ns:site1', None,
            f'{ASSET_DIR}/compute-asset.tar.gz',
            script_metadata)

    assets = [output_base, script]

    for asset in assets:
        client.store_asset(asset)


def add_initial_rules(
        client: InternalSiteRestClient, private_key: RSAPrivateKey) -> None:
    rules = [
            MayAccess(
                'site:party1_ns:site1',
                'asset:party1_ns:da.data.output_base:party1_ns:site1'),
            MayAccess(
                'site:party1_ns:site1',
                'asset:party1_ns:da.software.script1:party1_ns:site1'),
            ResultOfDataIn(
                'asset:party1_ns:da.data.output_base:party1_ns:site1', '*',
                'asset_collection:party1_ns:da.data.public'),
            ResultOfDataIn(
                'asset_collection:party1_ns:da.data.public', '*',
                'asset_collection:party1_ns:da.data.public'),
            ResultOfComputeIn(
                '*', 'asset:party1_ns:da.software.script1:party1_ns:site1',
                'asset_collection:party1_ns:da.data.results'),
            MayAccess(
                '*',
                'asset_collection:party1_ns:da.data.public'),
            MayAccess(
                'site:party1_ns:site1',
                'asset_collection:party1_ns:da.data.results')
            ]

    for rule in rules:
        rule.sign(private_key)
        client.add_rule(rule)


if __name__ == "__main__":
    private_key = generate_private_key(
            public_exponent=65537, key_size=2048, backend=default_backend())

    key_pem = private_key.public_key().public_bytes(
            encoding=Encoding.PEM,
            format=PublicFormat.SubjectPublicKeyInfo
            ).decode('ascii')

    print(f'Public key: {key_pem}')

    register(private_key.public_key())

    client = InternalSiteRestClient("site1", "http://site1:1080")
    add_initial_assets(client)
    add_initial_rules(client, private_key)
