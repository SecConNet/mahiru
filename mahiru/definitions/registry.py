"""Definitions of the contents of the central registry."""
from typing import cast, List, Optional

from cryptography.x509 import Certificate
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from mahiru.definitions.identifier import Identifier
from mahiru.definitions.signable import Signable
from mahiru.util import ComparesByValue


class RegisteredObject(ComparesByValue, Signable):
    """Base class for objects in the registry."""
    pass


class PartyDescription(RegisteredObject):
    """Describes a Party to the rest of the DDM.

    Attributes:
        id: Id of the party.
        namespace: The party's namespace.
        main_certificate: The party's certificate for verifying
            policies and registry records.
        user_ca_certificate: The party's certificate for signing user
            certificates for its users.
        user_certificates: Certificates for the party's users, for
            verifying workflow execution requests.

    """
    def __init__(
            self, party_id: Identifier, namespace: str,
            main_certificate: Certificate,
            user_ca_certificate: Certificate,
            user_certificates: List[Certificate],
            ) -> None:
        """Create a PartyDescription.

        Args:
            party_id: ID of the party.
            namespace: The party's namespace.
            main_certificate: The party's certificate for verifying
                policies and registry records.
            user_ca_certificate: The party's certificate for signing
                user certificates for its users.
            user_certificates: Certificates for the party's users, for
                verifying workflow execution requests.
        """
        if not isinstance(main_certificate.public_key(), Ed25519PublicKey):
            raise RuntimeError('Main certificate does not use ED25519')

        if not isinstance(user_ca_certificate.public_key(), Ed25519PublicKey):
            raise RuntimeError('User CA certificate does not use ED25519')

        for cert in user_certificates:
            if not isinstance(cert.public_key(), Ed25519PublicKey):
                raise RuntimeError('User certificate does not use ED25519')

        self.id = party_id
        self.namespace = namespace
        self.main_certificate = main_certificate
        self.user_ca_certificate = user_ca_certificate
        self.user_certificates = user_certificates

    def __repr__(self) -> str:
        """Returns a string representation of the object."""
        return f'PartyDescription({self.id}, {self.namespace})'

    def __hash__(self) -> int:
        """Hash the object."""
        return hash(self.signing_representation())

    def signing_representation(self) -> bytes:
        """Return a string of bytes representing the object.

        This adapts the Signable base class to this class.
        """
        main_cert_pem = self.main_certificate.public_bytes(
                Encoding.PEM).decode('ascii')
        user_ca_cert_pem = self.user_ca_certificate.public_bytes(
                Encoding.PEM).decode('ascii')
        base = (
            f'PartyDescription|{self.id}|{self.namespace}'
            f'|{main_cert_pem}|{user_ca_cert_pem}')
        for user_cert in self.user_certificates:
            user_cert_pem = user_cert.public_bytes(
                    Encoding.PEM).decode('ascii')
            base += f'|{user_cert_pem}'
        return base.encode('utf-8')

    def main_key(self) -> Ed25519PublicKey:
        """Returns the public key from the main certificate.

        This is here to avoid having to cast all over the place.
        """
        return cast(Ed25519PublicKey, self.main_certificate.public_key())


class SiteDescription(RegisteredObject):
    """Describes a site to the rest of the DDM.

    Attributes:
        id: ID of the site.
        owner_id: The party which owns this site.
        admin_id: The party which administrates this site.
        endpoint: This site's REST endpoint.
        https_certificate: This site's current certificate.
        has_runner: Whether the site has a runner.
        has_store: Whether the site has a store.
        has_policies: Whether the site serves policies.
    """
    def __init__(
            self,
            site_id: Identifier,
            owner_id: Identifier,
            admin_id: Identifier,
            endpoint: str,
            https_certificate: Certificate,
            has_runner: bool,
            has_store: bool,
            has_policies: bool
            ) -> None:
        """Create a SiteDescription.

        Args:
            site_id: Identifier of the site.
            owner_id: Identifier of the party which owns this site.
            admin_id: Identifier of the party which administrates this
                site.
            endpoint: URL of the REST endpoint of this site.
            https_certificate: This site's current certificate.
            has_runner: Whether the site has a runner.
            has_store: Whether the site has a store.
            has_policies: Whether the site serves policies.
        """
        self.id = site_id
        self.owner_id = owner_id
        self.admin_id = admin_id
        self.endpoint = endpoint
        self.https_certificate = https_certificate
        self.has_runner = has_runner
        self.has_store = has_store
        self.has_policies = has_policies

        if has_runner and not has_store:
            raise RuntimeError('Site with runner needs a store')

    def __repr__(self) -> str:
        """Returns a string representation of the object."""
        return f'SiteDescription({self.id})'

    def __hash__(self) -> int:
        """Hash the object."""
        return hash(self.signing_representation())

    def signing_representation(self) -> bytes:
        """Return a string of bytes representing the object.

        This adapts the Signable base class to this class.
        """
        https_cert_pem = self.https_certificate.public_bytes(
                Encoding.PEM).decode('ascii')
        return (
                f'SiteDescription|{self.id}|{self.owner_id}|{self.admin_id}'
                f'|{self.endpoint}|{https_cert_pem}'
                f'|{self.has_runner}|{self.has_store}|{self.has_policies}'
                ).encode('utf-8')
