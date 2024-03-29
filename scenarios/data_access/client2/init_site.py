#!/usr/bin/env python3
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import load_pem_private_key
import requests

from mahiru.definitions.assets import ComputeAsset, ComputeMetadata, DataAsset
from mahiru.definitions.registry import PartyDescription, SiteDescription
from mahiru.policy.rules import MayAccess, MayUse, ResultOfDataIn
from mahiru.rest.registry_client import RegistrationRestClient
from mahiru.rest.internal_client import InternalSiteRestClient


ASSET_DIR = Path.home() / 'mahiru' / 'assets'
CERTS_DIR = Path.home() / 'mahiru' / 'certs'
PRIVATE_DIR = Path.home() / 'mahiru' / 'private'


def register(main_key: Ed25519PrivateKey) -> None:
    """Registers the current party and site with the registry."""
    cert_file = CERTS_DIR / 'party2_main_cert.pem'
    with cert_file.open('rb') as f:
        main_cert = x509.load_pem_x509_certificate(f.read())

    cert_file = CERTS_DIR / 'party2_user_ca_cert.pem'
    with cert_file.open('rb') as f:
        user_ca_cert = x509.load_pem_x509_certificate(f.read())

    party_id = 'party:party2.mahiru.example.org:party2'
    namespace = 'party2.mahiru.example.org'
    party_desc = PartyDescription(
            party_id, namespace, main_cert, user_ca_cert, [])
    party_desc.sign(main_key)

    cert_file = CERTS_DIR / 'site2_https_cert.pem'
    with cert_file.open('rb') as f:
        https_cert = x509.load_pem_x509_certificate(f.read())

    site_id = 'site:party2.mahiru.example.org:site2'
    endpoint = 'https://site2.mahiru.example.org'
    site_desc = SiteDescription(
            site_id, party_id, party_id, endpoint, https_cert, True, True,
            True)
    site_desc.sign(main_key)

    client = RegistrationRestClient(
            'https://registry.mahiru.example.org',
            CERTS_DIR / 'trust_store.pem',
            (
                CERTS_DIR / 'site2_https_cert.pem',
                PRIVATE_DIR / 'site2_https_key.pem'))

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
            'asset:party2.mahiru.example.org:da.data.input'
            ':party2.mahiru.example.org:site2', None,
            f'{ASSET_DIR}/data-asset-input.tar.gz')

    client.store_asset(data_asset)


def add_initial_rules(
        client: InternalSiteRestClient, main_key: Ed25519PrivateKey) -> None:
    rules = [
            MayAccess(
                '*',
                'asset:party2.mahiru.example.org:da.data.input:'
                'party2.mahiru.example.org:site2'),
            ResultOfDataIn(
                'asset:party2.mahiru.example.org:da.data.input:'
                'party2.mahiru.example.org:site2',
                'asset:party1.mahiru.example.org:da.software.script1:'
                'party1.mahiru.example.org:site1', '*',
                'asset_collection:party2.mahiru.example.org:da.data.results'),
            MayAccess(
                'site:party2.mahiru.example.org:site2',
                'asset_collection:party2.mahiru.example.org:da.data.results'),
            MayAccess(
                'site:party1.mahiru.example.org:site1',
                'asset_collection:party2.mahiru.example.org:da.data.results'),
            MayUse(
                'party:party2.mahiru.example.org:party2',
                'asset_collection:party2.mahiru.example.org:da.data.results',
                'Any use'),
            MayUse(
                'party:party1.mahiru.example.org:party1',
                'asset_collection:party2.mahiru.example.org:da.data.results',
                'Any use')
            ]

    for rule in rules:
        rule.sign(main_key)
        client.add_rule(rule)


if __name__ == "__main__":
    key_file = PRIVATE_DIR / 'party2_main_key.pem'
    with key_file.open('rb') as f:
        main_key = load_pem_private_key(f.read(), None)

    register(main_key)

    client = InternalSiteRestClient(
            'party:party2.mahiru.example.org:party2',
            'site:party2.mahiru.example.org:site2',
            'https://site2.mahiru.example.org:1443',
            CERTS_DIR / 'trust_store.pem',
            (
                CERTS_DIR / 'party2_user1_cert.pem',
                PRIVATE_DIR / 'party2_user1_key.pem'))
    add_initial_assets(client)
    add_initial_rules(client, main_key)
