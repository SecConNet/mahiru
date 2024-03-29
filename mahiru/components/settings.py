"""Site configuration."""
from pathlib import Path
from typing import AnyStr, IO, List, Optional, Tuple, Union

import ruamel.yaml as yaml
import yatiml

from mahiru.definitions.identifier import Identifier


class NetworkSettings:
    """Whether and how to serve remote connections to assets.

    Attributes:
        enabled: Whether to allow remote connections to assets.
        external_ip: External IPv4 address we are available at.
        ports: Port range to use for serving incoming connections,
            as a length-2 list of the form [min, max].
    """
    def __init__(
            self, enabled: bool = False, external_ip: Optional[str] = None,
            ports: Optional[List[int]] = None) -> None:
        """Create a ConnectionSettings object.

        Args:
            enabled: Whether to allow remote connections to assets.
            external_ip: External IPv4 address we are available at.
            ports: Port range to use for serving incoming connections,
                as a length-2 list of the form [min, max].
        """
        if enabled:
            if not external_ip:
                raise RuntimeError('External IP not specified')
            for num in external_ip.split('.'):
                if int(num) < 0 or int(num) > 255:
                    raise RuntimeError('Invalid external IP address')

            if not ports:
                raise RuntimeError('No external port range specified')

            if len(ports) != 2:
                raise RuntimeError('Expected port range like [min, max]')

            if ports[0] > ports[1]:
                raise RuntimeError('Minimum must be <= maximum')

        self.enabled = enabled
        self.external_ip = external_ip
        self.ports = ports


class SiteConfiguration:
    """Configuration for a site.

    Attributes:
        site_id: Id of the site.
        namespace: Namespace controlled by the site's policy server.
        owner: Party owning the site.
        network_settings: Settings for external asset network
                connections.
        registry_endpoint: Registry endpoint location.
        trust_store: File with trusted certificates/trust anchors.
        client_cert: File with an HTTPS client certificate to use. This
                is the same file as we use for our HTTPS server.
        client_key: File with the key for client_cert.
        loglevel: Logging level to use, one of 'critical', 'error',
                'warning', 'info', or 'debug'.
    """
    def __init__(
            self,
            site_id: Identifier, namespace: str, owner: Identifier,
            network_settings: NetworkSettings,
            registry_endpoint: str,
            trust_store: Optional[Path] = None,
            client_cert: Optional[Path] = None,
            client_key: Optional[Path] = None,
            loglevel: str = 'info'
            ) -> None:
        """Create a SiteConfiguration object.

        Args:
            site_id: Id of the site
            namespace: Namespace controlled by the site's policy server.
            owner: Id of the party owning the site, e.g.
                    "party:namespace:name".
            network_settings: Settings for external asset network
                    connections.
            registry_endpoint: Registry endpoint location.
            trust_store: File with trusted certificates/trust anchors.
            client_cert: File with an HTTPS client certificate to use.
            client_key: File with the key for client_cert.
            loglevel: Logging level to use, one of 'critical', 'error',
                    'warning', 'info', or 'debug'.
        """
        if owner.kind() != 'party':
            raise ValueError(
                    'Expected a name of the form "party:<namespace>:<name>"')

        if client_cert and not client_key:
            raise ValueError(
                    'A client certificate was given but not the corresponding'
                    ' key. Please specify either neither or both.')

        if client_key and not client_cert:
            raise ValueError(
                    'A client key was given but not the corresponding'
                    ' certificate. Please specify either neither or both.')

        self.site_id = site_id
        self.namespace = namespace
        self.owner = owner
        self.network_settings = network_settings
        self.registry_endpoint = registry_endpoint
        self.trust_store = trust_store
        self.client_cert = client_cert
        self.client_key = client_key
        self.loglevel = loglevel

    def client_creds(self) -> Optional[Tuple[Path, Path]]:
        """Get the HTTPS client credentials.

        This returns the certificate and the corresponding key as a
        tuple.
        """
        if self.client_cert and self.client_key:
            return self.client_cert, self.client_key
        return None

    @classmethod
    def _yatiml_recognize(cls, node: yatiml.UnknownNode) -> None:
        pass

    @classmethod
    def _yatiml_savorize(cls, node: yatiml.Node) -> None:
        if node.is_mapping():
            if not node.has_attribute('network_settings'):
                ac_node = yaml.MappingNode('tag:yaml.org,2002:map', [])
                node.set_attribute('network_settings', ac_node)


_load_settings = yatiml.load_function(
        SiteConfiguration, NetworkSettings, Identifier)


_default_config_location = Path('/etc/mahiru/mahiru.conf')


def load_settings(
        source: Union[str, Path, IO[AnyStr]] = _default_config_location
        ) -> SiteConfiguration:
    """Load settings from a source.

    The source can be a string containing YAML, pathlib.Path containing
    a path to a file to load, or a stream (e.g. an open file handle
    returned by open()).

    Args:
        source: The source to load from.

    Returns:
        An object loaded from the file.

    Raises:
        yatiml.RecognitionError: If the input is invalid.
    """
    return _load_settings(source)
