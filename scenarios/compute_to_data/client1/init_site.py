#!/usr/bin/env python3

from pathlib import Path
from typing import cast

from cryptography import x509
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import load_pem_private_key
import requests

from mahiru.definitions.assets import ComputeAsset, ComputeMetadata, DataAsset
from mahiru.definitions.registry import PartyDescription, SiteDescription
from mahiru.policy.rules import (
        MayAccess, MayUse, ResultOfComputeIn, ResultOfDataIn)
from mahiru.rest.registry_client import RegistrationRestClient
from mahiru.rest.internal_client import InternalSiteRestClient


ASSET_DIR = Path.home() / 'mahiru' / 'assets'
CERTS_DIR = Path.home() / 'mahiru' / 'certs'
PRIVATE_DIR = Path.home() / 'mahiru' / 'private'


def register(main_key: Ed25519PrivateKey) -> None:
    """Registers the current party and site with the registry."""
    cert_file = CERTS_DIR / 'party1_main_cert.pem'
    with cert_file.open('rb') as f:
        main_cert = x509.load_pem_x509_certificate(f.read())

    cert_file = CERTS_DIR / 'party1_user_ca_cert.pem'
    with cert_file.open('rb') as f:
        user_ca_cert = x509.load_pem_x509_certificate(f.read())

    party_id = 'party:party1.mahiru.example.org:party1'
    namespace = 'party1.mahiru.example.org'
    party_desc = PartyDescription(
            party_id, namespace, main_cert, user_ca_cert, [])
    party_desc.sign(main_key)

    cert_file = CERTS_DIR / 'site1_https_cert.pem'
    with cert_file.open('rb') as f:
        https_cert = x509.load_pem_x509_certificate(f.read())

    site_id = 'site:party1.mahiru.example.org:site1'
    endpoint = 'https://site1.mahiru.example.org'
    site_desc = SiteDescription(
            site_id, party_id, party_id, endpoint, https_cert, True, True,
            True)
    site_desc.sign(main_key)

    client = RegistrationRestClient(
            'https://registry.mahiru.example.org',
            CERTS_DIR / 'trust_store.pem',
            (
                CERTS_DIR / 'site1_https_cert.pem',
                PRIVATE_DIR / 'site1_https_key.pem'))

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
            'asset:party1.mahiru.example.org:ctd.data.output_base'
            ':party1.mahiru.example.org:site1', None,
            f'{ASSET_DIR}/data-asset-base.tar.gz')

    script_metadata = ComputeMetadata({
            'output0': 'asset:party1.mahiru.example.org:ctd.data.output_base'
            ':party1.mahiru.example.org:site1'})

    script = ComputeAsset(
            'asset:party1.mahiru.example.org:ctd.software.script1'
            ':party1.mahiru.example.org:site1', None,
            f'{ASSET_DIR}/compute-asset.tar.gz',
            script_metadata)

    assets = [output_base, script]

    for asset in assets:
        client.store_asset(asset)


def add_initial_rules(
        client: InternalSiteRestClient, main_key: Ed25519PrivateKey) -> None:
    rules = [
            MayAccess(
                'site:party1.mahiru.example.org:site1',
                'asset:party1.mahiru.example.org:ctd.data.output_base'
                ':party1.mahiru.example.org:site1'),
            MayAccess(
                'site:party2.mahiru.example.org:site2',
                'asset:party1.mahiru.example.org:ctd.data.output_base'
                ':party1.mahiru.example.org:site1'),
            MayAccess(
                'site:party1.mahiru.example.org:site1',
                'asset:party1.mahiru.example.org:ctd.software.script1'
                ':party1.mahiru.example.org:site1'),
            MayAccess(
                'site:party2.mahiru.example.org:site2',
                'asset:party1.mahiru.example.org:ctd.software.script1'
                ':party1.mahiru.example.org:site1'),
            ResultOfDataIn(
                'asset:party1.mahiru.example.org:ctd.data.output_base'
                ':party1.mahiru.example.org:site1', '*',
                '*',
                'asset_collection:party1.mahiru.example.org:ctd.data.public'),
            ResultOfDataIn(
                'asset_collection:party1.mahiru.example.org:ctd.data.public',
                '*', '*',
                'asset_collection:party1.mahiru.example.org:ctd.data.public'),
            ResultOfComputeIn(
                '*',
                'asset:party1.mahiru.example.org:ctd.software.script1'
                ':party1.mahiru.example.org:site1',
                '*',
                'asset_collection:party1.mahiru.example.org:ctd.data.results'),
            MayAccess(
                'site:party1.mahiru.example.org:site1',
                'asset_collection:party1.mahiru.example.org:ctd.data.public'),
            MayAccess(
                'site:party2.mahiru.example.org:site2',
                'asset_collection:party1.mahiru.example.org:ctd.data.public'),
            MayAccess(
                'site:party1.mahiru.example.org:site1',
                'asset_collection:party1.mahiru.example.org:ctd.data.results'),
            MayAccess(
                'site:party2.mahiru.example.org:site2',
                'asset_collection:party1.mahiru.example.org:ctd.data.results'),
            MayUse(
                'party:party1.mahiru.example.org:party1',
                'asset_collection:party1.mahiru.example.org:ctd.data.public',
                'Any use'),
            MayUse(
                'party:party2.mahiru.example.org:party2',
                'asset_collection:party1.mahiru.example.org:ctd.data.public',
                'Any use'),
            MayUse(
                'party:party1.mahiru.example.org:party1',
                'asset_collection:party1.mahiru.example.org:ctd.data.results',
                'Any use')
            ]

    for rule in rules:
        rule.sign(main_key)
        client.add_rule(rule)


if __name__ == "__main__":
    key_file = PRIVATE_DIR / 'party1_main_key.pem'
    with key_file.open('rb') as f:
        main_key = load_pem_private_key(f.read(), None)

    register(main_key)

    client = InternalSiteRestClient(
            'party:party1.mahiru.example.org:party1',
            'site:party1.mahiru.example.org:site1',
            'https://site1.mahiru.example.org:1443',
            CERTS_DIR / 'trust_store.pem',
            (
                CERTS_DIR / 'party1_user1_cert.pem',
                PRIVATE_DIR / 'party1_user1_key.pem'))
    add_initial_assets(client)
    add_initial_rules(client, main_key)
