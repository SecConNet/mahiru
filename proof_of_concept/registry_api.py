"""REST-style API for central registry."""
from typing import Any

from falcon import App, HTTP_201, HTTP_400, HTTP_409, Request, Response

from proof_of_concept.definitions import PartyDescription
from proof_of_concept.serialization import Deserializer, ValidationError
from proof_of_concept.registry import Registry


class PartyRegistration:
    """A handler for the /parties endpoint."""
    def __init__(self, registry: Registry, deserializer: Deserializer) -> None:
        """Create a PartyRegistration handler.

        Args:
            registry: The registry to send requests to.
            deserializer: A Deserializer to (de)serialise objects with.
        """
        self._registry = registry
        self._deserializer = deserializer

    def on_post(self, request: Request, response: Response) -> None:
        """Handle a party registration request.

        Args:
            request: The submitted request.
            response: A response object to configure.

        """
        try:
            self._registry.register_party(self._deserializer(
                PartyDescription, request.media))
            response.status = HTTP_201
            response.body = 'Created'
        except ValidationError:
            response.status = HTTP_400
            response.body = 'Invalid request'
        except RuntimeError:
            response.status = HTTP_409
            response.body = 'Party already exists'


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

        deserializer = Deserializer()

        party_registration = PartyRegistration(registry, deserializer)
        self.app.add_route('/parties', party_registration)
