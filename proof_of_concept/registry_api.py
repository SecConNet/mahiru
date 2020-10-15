"""REST-style API for central registry."""
import logging
from pathlib import Path
from typing import Any

from falcon import App, HTTP_201, HTTP_400, HTTP_409, Request, Response
import ruamel.yaml as yaml

from proof_of_concept.definitions import (
        PartyDescription, RegisteredObject, SiteDescription)
from proof_of_concept.serialization import (
        deserialize_party_description, deserialize_site_description)
from proof_of_concept.registry import Registry
from proof_of_concept.replication_rest import ReplicationHandler
from proof_of_concept.validation import Validator, ValidationError


logger = logging.getLogger(__name__)


class PartyRegistration:
    """A handler for the /parties endpoint."""
    def __init__(
            self,
            registry: Registry,
            validator: Validator) -> None:
        """Create a PartyRegistration handler.

        Args:
            registry: The registry to send requests to.
            validator: A validator to validate input with.
        """
        self._registry = registry
        self._validator = validator

    def on_post(self, request: Request, response: Response) -> None:
        """Handle a party registration request.

        Args:
            request: The submitted request.
            response: A response object to configure.

        """
        try:
            self._validator.validate('Party', request.media)
            self._registry.register_party(
                    deserialize_party_description(request.media))
            response.status = HTTP_201
            response.body = 'Created'
        except ValidationError:
            response.status = HTTP_400
            response.body = 'Invalid request'
        except RuntimeError:
            response.status = HTTP_409
            response.body = 'Party already exists'


class SiteRegistration:
    """A handler for the /sites endpoint."""
    def __init__(self, registry: Registry, validator: Validator) -> None:
        """Create a SiteRegistration handler.

        Args:
            registry: The registry to send requests to.
            validator: A Validator to validate objects with.
        """
        self._registry = registry
        self._validator = validator

    def on_post(self, request: Request, response: Response) -> None:
        """Handle a site registration request.

        Args:
            request: The submitted request.
            response: A response object to configure.

        """
        try:
            self._validator.validate('Site', request.media)
            self._registry.register_site(
                    deserialize_site_description(request.media))
            response.status = HTTP_201
            response.body = 'Created'
        except ValidationError as e:
            logger.error(f'Invalid site description {e}')
            response.status = HTTP_400
            response.body = 'Invalid request'
        except RuntimeError as e:
            logger.error(f'Tried to reregister site {e}')
            response.status = HTTP_409
            response.body = 'Site already exists'


class RegistryApi:
    """The complete Registry REST API.

    Attributes:
        app: The WSGI application object.

    """
    def __init__(self, registry: Registry) -> None:
        """Create a RegistryApi instance.

        Args:
            registry: The registry to serve for.

        """
        self.app = App()

        registry_api_file = Path(__file__).parent / 'registry_api.yaml'
        with open(registry_api_file, 'r') as f:
            registry_api_def = yaml.safe_load(f.read())

        validator = Validator(registry_api_def)

        party_registration = PartyRegistration(registry, validator)
        self.app.add_route('/parties', party_registration)

        site_registration = SiteRegistration(registry, validator)
        self.app.add_route('/sites', site_registration)

        registry_replication = ReplicationHandler[RegisteredObject](
                registry.replication_server)
        self.app.add_route('/updates', registry_replication)
