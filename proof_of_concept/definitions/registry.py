"""Definitions of the contents of the central registry."""
from typing import Optional

from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey


class RegisteredObject:
    """Base class for objects in the registry."""
    pass


class PartyDescription(RegisteredObject):
    """Describes a Party to the rest of the DDM.

    Attributes:
        name: Name of the party.
        public_key: The party's public key for signing rules.

    """
    def __init__(self, name: str, public_key: RSAPublicKey) -> None:
        """Create a PartyDescription.

        Args:
            name: Name of the party.
            public_key: The party's public key for signing rules.
        """
        self.name = name
        self.public_key = public_key

    def __repr__(self) -> str:
        """Returns a string representation of the object."""
        return f'PartyDescription({self.name})'


class SiteDescription(RegisteredObject):
    """Describes a site to the rest of the DDM.

    Attributes:
        name: Name of the site.
        owner_name: Name of the party which owns this site.
        admin_name: Name of the party which administrates this site.
        endpoint: This site's REST endpoint.
        runner: Whether the site has a runner.
        store: Whether the site has a store.
        namespace: The namespace managed by this site's policy server,
            if any.

    """
    def __init__(
            self,
            name: str,
            owner_name: str,
            admin_name: str,
            endpoint: str,
            runner: bool,
            store: bool,
            namespace: Optional[str]
            ) -> None:
        """Create a SiteDescription.

        Args:
            name: Name of the site.
            owner_name: Name of the party which owns this site.
            admin_name: Name of the party which administrates this site.
            endpoint: URL of the REST endpoint of this site.
            runner: Whether the site has a runner.
            store: Whether the site has a store.
            namespace: The namespace managed by this site's policy
                server, if any.

        """
        self.name = name
        self.owner_name = owner_name
        self.admin_name = admin_name
        self.endpoint = endpoint
        self.runner = runner
        self.store = store
        self.namespace = namespace

        if runner and not store:
            raise RuntimeError('Site with runner needs a store')

    def __repr__(self) -> str:
        """Returns a string representation of the object."""
        return f'SiteDescription({self.name})'
