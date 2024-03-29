from copy import copy

from mahiru.definitions.identifier import Identifier
from mahiru.definitions.registry import PartyDescription, SiteDescription
from mahiru.registry.registry import Registry


def test_parties_are_values(
        party1_main_key, party1_main_certificate, party1_user_ca_certificate):
    # If a party is removed and then reinserted into the registry
    # with the same properties, then the reinserted version of the
    # party should be considered the same party, and if a replica
    # update is requested from a version in which the party was present
    # to a version in which it is again present, then the party should
    # not show up in the update at all.
    registry = Registry()
    party1 = PartyDescription(
            Identifier('party:party1_ns:party1'), 'ns',
            party1_main_certificate, party1_user_ca_certificate, [])
    party1.sign(party1_main_key)

    registry.register_party(copy(party1))
    update1 = registry.get_updates_since(0)
    assert update1.created == {party1}
    assert not update1.deleted

    registry.deregister_party(copy(party1.id))
    registry.register_party(copy(party1))
    update2 = registry.get_updates_since(update1.to_version)
    assert not update2.created
    assert not update2.deleted


def test_sites_are_values(
        party1_main_key, party1_main_certificate, party1_user_ca_certificate,
        site1_https_certificate):
    # See test_parties_are_values, but for a site.
    registry = Registry()
    party1 = PartyDescription(
            Identifier('party:party1_ns:party1'), 'ns',
            party1_main_certificate, party1_user_ca_certificate, [])
    party1.sign(party1_main_key)
    registry.register_party(copy(party1))

    site1 = SiteDescription(
            Identifier('site:party1_ns:site'),
            Identifier('party:party1_ns:party1'),
            Identifier('party:party1_ns:party1'),
            'https://party1.example.com/mahiru',
            site1_https_certificate,
            True, True, 'party1_ns')
    site1.sign(party1_main_key)

    registry.register_site(copy(site1))
    update1 = registry.get_updates_since(0)
    assert update1.created == {party1, site1}
    assert not update1.deleted

    registry.deregister_site(copy(site1.id))
    registry.register_site(copy(site1))
    update2 = registry.get_updates_since(update1.to_version)
    assert not update2.created
    assert not update2.deleted
