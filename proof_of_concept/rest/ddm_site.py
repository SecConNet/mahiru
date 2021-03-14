"""REST-style API for a site."""
from copy import copy
import logging
from pathlib import Path
from threading import Thread
from socketserver import ThreadingMixIn
from urllib.parse import quote
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer

from falcon import App, HTTP_200, HTTP_400, HTTP_404, Request, Response
import ruamel.yaml as yaml
import yatiml

from proof_of_concept.components.ddm_site import Site
from proof_of_concept.components.registry_client import RegistryClient
from proof_of_concept.definitions.identifier import Identifier
from proof_of_concept.definitions.interfaces import IAssetStore, IStepRunner
from proof_of_concept.definitions.policy import Rule
from proof_of_concept.definitions.workflows import JobSubmission
from proof_of_concept.policy.replication import PolicyStore
from proof_of_concept.rest.replication import ReplicationHandler
from proof_of_concept.rest.serialization import deserialize, serialize
from proof_of_concept.rest.validation import Validator, ValidationError


logger = logging.getLogger(__name__)


class AssetAccessHandler:
    """A handler for the external /assets endpoint."""
    def __init__(self, store: IAssetStore) -> None:
        """Create an AssetAccessHandler handler.

        Args:
            store: The asset store to send requests to.
        """
        self._store = store

    def on_get(
            self, request: Request, response: Response, asset_id: str
            ) -> None:
        """Handle request for an asset.

        Args:
            request: The submitted request.
            response: A response object to configure.
            asset_id: The id of the requested asset

        """
        logger.info(f'Asset access request, store = {self._store}')
        if 'requester' not in request.params:
            logger.info(f'Invalid asset access request')
            response.status = HTTP_400
            response.body = 'Invalid request'
        else:
            logger.info(
                    f'Received request for asset {asset_id} from'
                    f' {request.params["requester"]}')
            try:
                asset = copy(self._store.retrieve(
                        Identifier(asset_id), request.params['requester']))
                # Send URL instead of local file location
                if asset.image_location is not None:
                    prefix = request.forwarded_prefix
                    path = quote(request.path, safe='/')
                    asset.image_location = f'{prefix}{path}/image'
                logger.info(
                        f'Sending with asset location {asset.image_location}')
                response.status = HTTP_200
                response.media = serialize(asset)
            except KeyError:
                logger.info(f'Asset {asset_id} not found')
                response.status = HTTP_404
                response.body = 'Asset not found'
            except RuntimeError:
                # This is permission denied, but we return a 404 to
                # avoid information-leaking the existence of any
                # particular assets.
                logger.info(
                        f'Asset {asset_id} not available for user'
                        f' {request.params["requester"]}')
                response.status = HTTP_404
                response.body = 'Asset not found'


class AssetImageAccessHandler:
    """A handler for the external /assets/{assetId}/image endpoints."""
    def __init__(self, store: IAssetStore) -> None:
        """Create an AssetImageAccessHandler handler.

        Args:
            store: The asset store to send requests to.
        """
        self._store = store

    def on_get(
            self, request: Request, response: Response, asset_id: str
            ) -> None:
        """Handle request for an asset image.

        Args:
            request: The submitted request.
            response: A response object to configure.
            asset_id: The id of the requested asset

        """
        logger.info(f'Asset image request, store = {self._store}')
        if 'requester' not in request.params:
            logger.info(f'Invalid asset access request')
            response.status = HTTP_400
            response.body = 'Invalid request'
        else:
            logger.info(
                    f'Received request for asset {asset_id} from'
                    f' {request.params["requester"]}')
            try:
                asset = self._store.retrieve(
                        Identifier(asset_id), request.params['requester'])
                if asset.image_location is None:
                    raise KeyError()
                response.status = HTTP_200
                response.content_type = 'application/x-tar'
                logger.info(f'Reading image from {asset.image_location}')
                image_path = Path(asset.image_location)
                image_size = image_path.stat().st_size
                image_stream = image_path.open('rb')
                response.set_stream(image_stream, image_size)
            except KeyError:
                logger.info(f'Asset {asset_id} not found')
                response.status = HTTP_404
                response.body = 'Asset not found'
            except RuntimeError:
                # This is permission denied, but we return a 404 to
                # avoid information-leaking the existence of any
                # particular assets.
                logger.info(
                        f'Asset {asset_id} not available for user'
                        f' {request.params["requester"]}')
                response.status = HTTP_404
                response.body = 'Asset not found'


class WorkflowExecutionHandler:
    """A handler for the external /jobs endpoint."""
    def __init__(
            self, runner: IStepRunner, validator: Validator
            ) -> None:
        """Create a WorkflowExecutionHandler handler.

        Args:
            runner: The runner to send requests to.
            validator: A Validator to validate requests with.
        """
        self._runner = runner
        self._validator = validator

    def on_post(self, request: Request, response: Response) -> None:
        """Handle request to execute part of a workflow.

        Args:
            request: The submitted request.
            response: A response object to configure.

        """
        try:
            logger.info(f'Received execution request: {request.media}')
            self._validator.validate('JobSubmission', request.media)
            submission = deserialize(JobSubmission, request.media)
            self._runner.execute_job(submission)
        except ValidationError:
            logger.warning(f'Invalid execution request: {request.media}')
            response.status = HTTP_400
            response.body = 'Invalid request'


class SiteRestApi:
    """The complete Site REST API.

    Attributes:
        app: The WSGI application object.

    """
    def __init__(
            self,
            policy_store: PolicyStore,
            asset_store: IAssetStore,
            runner: IStepRunner) -> None:
        """Create a SiteRestApi instance.

        Args:
            policy_store: The store to offer policy updates from.
            asset_store: The store to serve assets from.
            runner: The workflow runner to send requests to.

        """
        self.app = App()

        site_api_file = Path(__file__).parent / 'site_api.yaml'
        with open(site_api_file, 'r') as f:
            site_api_def = yaml.safe_load(f.read())

        validator = Validator(site_api_def)

        rule_replication = ReplicationHandler[Rule](policy_store)
        self.app.add_route('/external/rules/updates', rule_replication)

        asset_access = AssetAccessHandler(asset_store)
        self.app.add_route('/external/assets/{asset_id}', asset_access)

        asset_image_access = AssetImageAccessHandler(asset_store)
        self.app.add_route(
                '/external/assets/{asset_id}/image', asset_image_access)

        workflow_execution = WorkflowExecutionHandler(runner, validator)
        self.app.add_route('/external/jobs', workflow_execution)


class ThreadingWSGIServer (ThreadingMixIn, WSGIServer):
    """Threading version of a simple WSGI server."""
    pass


class SiteServer:
    """An HTTP server serving a SiteRestApi.

    Make sure to call `close()` when you're done, or the program will
    not shut down because the background thread will still be running.

    Attributes:
        endpoint: The HTTP endpoint at which the server can be reached.

    """
    def __init__(self, api: SiteRestApi) -> None:
        """Create a SiteServer serving an API.

        This starts a background thread with an HTTP server.

        Args:
            api: The API to serve.

        """
        self._server = ThreadingWSGIServer(('0.0.0.0', 0), WSGIRequestHandler)
        self._server.set_app(api.app)

        self._thread = Thread(
                target=self._server.serve_forever,
                name='SiteServer')
        self._thread.start()

        self.external_endpoint = (
                f'http://{self._server.server_name}'
                f':{self._server.server_port}/external')
        logger.info(f'Site server listening on {self.external_endpoint}')

    def close(self) -> None:
        """Stop the server thread."""
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()


class Settings:
    """Settings for a site.

    Attributes:
        name: Name of the site.
        namespace: Namespace controlled by the site's policy server.
        owner: Party owning the site.
        registry_endpoint: Registry endpoint location.
    """
    def __init__(
            self,
            name: str, namespace: str, owner: Identifier,
            registry_endpoint: str
            ) -> None:
        """Create a Settings object.

        Args:
            name: Name of the site.
            namespace: Namespace controlled by the site's policy server.
            owner: Party owning the site.
            registry_endpoint: Registry endpoint location.
        """
        self.name = name
        self.namespace = namespace
        self.owner = owner
        self.registry_endpoint = registry_endpoint


load_settings = yatiml.load_function(Settings, Identifier)


default_config_location = Path('/etc/mahiru/mahiru.conf')


def wsgi_app() -> App:
    """Creates a WSGI app for a WSGI runner."""
    settings = load_settings(default_config_location)

    registry_client = RegistryClient(settings.registry_endpoint)
    site = Site(
            settings.name, settings.owner, settings.namespace, [], [],
            registry_client)
    return SiteRestApi(site.policy_store, site.store, site.runner).app
