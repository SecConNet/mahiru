"""REST-style API for central registry."""
import logging
from pathlib import Path
from threading import Thread
from typing import Type
from wsgiref.simple_server import WSGIServer, WSGIRequestHandler

from falcon import (
        App, HTTP_200, HTTP_201, HTTP_400, HTTP_404, HTTP_409, Request,
        Response)
import ruamel.yaml as yaml

from mahiru.definitions.errors import ValidationError
from mahiru.definitions.identifier import Identifier
from mahiru.definitions.interfaces import IRegistration
from mahiru.definitions.registry import (
        PartyDescription, RegisteredObject, SiteDescription)
from mahiru.registry.registry import Registry
from mahiru.rest.replication import ReplicationHandler
from mahiru.rest.serialization import deserialize
from mahiru.rest.validation import validate_json


logger = logging.getLogger(__name__)


class PartyRegistrationHandler:
    """A handler for the /parties endpoint."""
    def __init__(
            self,
            registry: IRegistration) -> None:
        """Create a PartyRegistrationHandler handler.

        Args:
            registry: The registry to send requests to.
        """
        self._registry = registry

    def on_post(self, request: Request, response: Response) -> None:
        """Handle a party registration request.

        Args:
            request: The submitted request.
            response: A response object to configure.

        """
        try:
            validate_json('Party', request.media)
            self._registry.register_party(
                    deserialize(PartyDescription, request.media))
            response.status = HTTP_201
            response.body = 'Created'
        except ValidationError as e:
            logger.error(f'Invalid party description {e}')
            response.status = HTTP_400
            response.body = 'Invalid request'
        except RuntimeError as e:
            logger.error(f'Error registering party {e}')
            response.status = HTTP_409
            response.body = 'Error registering party'

    def on_delete(
            self, request: Request, response: Response, id: str) -> None:
        """Handle a party deregistration request.

        Args:
            request: The submitted request.
            response: A response object to configure
            id: Identifier of the party to deregister.

        """
        try:
            self._registry.deregister_party(Identifier(id))
            response.status = HTTP_200
            response.body = 'Deleted'
        except KeyError as e:
            logger.error(f'Request to deregister unknown party: {e}')
            response.status = HTTP_404
            response.body = 'Not found'


class SiteRegistrationHandler:
    """A handler for the /sites endpoint."""
    def __init__(self, registry: IRegistration) -> None:
        """Create a SiteRegistrationHandler handler.

        Args:
            registry: The registry to send requests to.
        """
        self._registry = registry

    def on_post(self, request: Request, response: Response) -> None:
        """Handle a site registration request.

        Args:
            request: The submitted request.
            response: A response object to configure.

        """
        try:
            validate_json('Site', request.media)
            self._registry.register_site(
                    deserialize(SiteDescription, request.media))
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

    def on_delete(
            self, request: Request, response: Response, id: str) -> None:
        """Handle a site deregistration request.

        Args:
            request: The submitted request.
            response: A response object to configure.
            id: Identifier of the site to deregister.

        """
        try:
            self._registry.deregister_site(Identifier(id))
            response.status = HTTP_200
            response.body = 'Deleted'
        except KeyError as e:
            logger.error(f'Request to deregister unknown site: {e}')
            response.status = HTTP_404
            response.body = 'Not found'


class RegistryRestApi:
    """The complete Registry REST API.

    Attributes:
        app: The WSGI application object.

    """
    def __init__(self, registry: Registry) -> None:
        """Create a RegistryRestApi instance.

        Args:
            registry: The registry to serve for.

        """
        self.app = App()

        registry_api_file = Path(__file__).parent / 'registry_api.yaml'
        with open(registry_api_file, 'r') as f:
            registry_api_def = yaml.safe_load(f.read())

        party_registration = PartyRegistrationHandler(registry)
        self.app.add_route('/parties', party_registration)
        self.app.add_route('/parties/{id}', party_registration)

        site_registration = SiteRegistrationHandler(registry)
        self.app.add_route('/sites', site_registration)
        self.app.add_route('/sites/{id}', site_registration)

        registry_replication = ReplicationHandler[RegisteredObject](registry)
        self.app.add_route('/updates', registry_replication)


class RegistryServer:
    """An HTTP server serving the registry API."""
    def __init__(
            self, api: RegistryRestApi,
            server_type: Type[WSGIServer] = WSGIServer
            ) -> None:
        """Create a RegistryServer.

        This starts a background thread with an HTTP server. It will
        listen on all local interfaces on port 4413.

        Args:
            api: The API to serve.
            server_type: The server class to use.
        """
        self._server = server_type(('0.0.0.0', 4413), WSGIRequestHandler)
        self._server.set_app(api.app)
        self._thread = Thread(
                target=self._server.serve_forever,
                name='RegistryServer')
        self._thread.start()

    def close(self) -> None:
        """Stop the server thread."""
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()


def wsgi_app() -> App:
    """Creates a WSGI app for a WSGI runner."""
    return RegistryRestApi(Registry()).app
