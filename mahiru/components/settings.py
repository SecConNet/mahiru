"""Site configuration."""
from pathlib import Path
from typing import AnyStr, IO, Union

import ruamel.yaml as yaml
import yatiml

from mahiru.definitions.identifier import Identifier


class Settings:
    """Settings for a site.

    Attributes:
        name: Name of the site.
        namespace: Namespace controlled by the site's policy server.
        owner: Party owning the site.
        registry_endpoint: Registry endpoint location.
        loglevel: Logging level to use, one of 'critical', 'error',
                'warning', 'info', or 'debug'.
    """
    def __init__(
            self,
            name: str, namespace: str, owner: Identifier,
            registry_endpoint: str, loglevel: str = 'info'
            ) -> None:
        """Create a Settings object.

        Args:
            name: Name of the site (without namespace or tag).
            namespace: Namespace controlled by the site's policy server.
            owner: Id of the party owning the site, e.g.
                    "party:namespace:name".
            registry_endpoint: Registry endpoint location.
            loglevel: Logging level to use, one of 'critical', 'error',
                    'warning', 'info', or 'debug'.
        """
        self.name = name
        self.namespace = namespace
        self.owner = owner
        self.registry_endpoint = registry_endpoint
        self.loglevel = loglevel


_load_settings = yatiml.load_function(Settings, Identifier)


_default_config_location = Path('/etc/mahiru/mahiru.conf')


def load_settings(
        source: Union[str, Path, IO[AnyStr]] = _default_config_location
        ) -> Settings:
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
