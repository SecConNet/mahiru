#!/usr/bin/env python3
from pathlib import Path

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric.rsa import (
        generate_private_key, RSAPrivateKey, RSAPublicKey)
import requests

from mahiru.definitions.assets import ComputeAsset, ComputeMetadata, DataAsset
from mahiru.definitions.registry import PartyDescription, SiteDescription
from mahiru.policy.rules import MayAccess, ResultOfDataIn
from mahiru.rest.registry_client import RegistrationRestClient
from mahiru.rest.internal_client import InternalSiteRestClient


ASSET_DIR = Path.home() / 'mahiru' / 'assets'


def register(public_key: RSAPublicKey) -> None:
    """Registers the current party and site with the registry."""
    party_id = 'party:party2_ns:party2'
    party_desc = PartyDescription(party_id, public_key)

    site_id = 'site:party2_ns:site2'
    endpoint = 'http://site2'
    namespace = 'party2_ns'
    site_desc = SiteDescription(
            site_id, party_id, party_id, endpoint, True, True, namespace)

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
    data_asset = DataAsset(
            'asset:party2_ns:ctd.data.input:party2_ns:site2', None,
            f'{ASSET_DIR}/data-asset-input.tar.gz')

    client.store_asset(data_asset)


def add_initial_rules(
        client: InternalSiteRestClient, private_key: RSAPrivateKey) -> None:
    rules = [
            MayAccess(
                'site:party2_ns:site2',
                'asset:party2_ns:ctd.data.input:party2_ns:site2'),
            ResultOfDataIn(
                'asset:party2_ns:ctd.data.input:party2_ns:site2',
                'asset:party1_ns:ctd.software.script1:party1_ns:site1',
                'asset_collection:party2_ns:ctd.data.results'),
            MayAccess(
                'site:party2_ns:site2',
                'asset_collection:party2_ns:ctd.data.results'),
            MayAccess(
                'site:party1_ns:site1',
                'asset_collection:party2_ns:ctd.data.results')
            ]

    for rule in rules:
        rule.sign(private_key)
        client.add_rule(rule)


if __name__ == "__main__":
    private_key = generate_private_key(
            public_exponent=65537, key_size=2048, backend=default_backend())

    register(private_key.public_key())

    client = InternalSiteRestClient("site2", "http://site2:1080")
    add_initial_assets(client)
    add_initial_rules(client, private_key)
