"""REST-style API for a site."""
from copy import copy
import logging
from pathlib import Path
from socketserver import ThreadingMixIn
from tempfile import NamedTemporaryFile
from threading import Thread
from typing import Dict
from urllib.parse import quote
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer

from falcon import (
        App, HTTP_200, HTTP_201, HTTP_204, HTTP_303, HTTP_400, HTTP_403,
        HTTP_404, Request, Response)
from jsonschema import ValidationError
import ruamel.yaml as yaml
import yatiml

from mahiru.components.ddm_site import Site
from mahiru.components.registry_client import RegistryClient
from mahiru.components.settings import load_settings
from mahiru.components.orchestration import WorkflowOrchestrator
from mahiru.definitions.assets import Asset
from mahiru.definitions.connections import ConnectionRequest
from mahiru.definitions.execution import JobResult
from mahiru.definitions.identifier import Identifier
from mahiru.definitions.interfaces import IAssetStore, IStepRunner
from mahiru.definitions.policy import Rule
from mahiru.definitions.workflows import ExecutionRequest, Job
from mahiru.policy.replication import PolicyStore
from mahiru.rest.registry_client import RegistryRestClient
from mahiru.rest.replication import ReplicationHandler
from mahiru.rest.serialization import deserialize, serialize
from mahiru.rest.validation import validate_json, ValidationError


logger = logging.getLogger(__name__)


def _request_url(request: Request) -> str:
    """Obtain the URL for the current request.

    This URL-quotes the path part of the URL, encoding everything
    except /.

    If we are running behind a reverse proxy, then what gunicorn thinks
    is our external location is actually an internal one. This function
    returns the actual external URL of the current request, excluding
    the parameters. If we're not behind a reverse proxy, then that's
    just the normal scheme + host + path, if we are behind a reverse
    proxy then the following headers must be set:

        X-Forwarded-Proto: The protocol used, http or https
        X-Forwarded-Host: The outside host part
        X-Forwarded-Path: The outside path

    Note that X-Forwarded-Path is very non-standard. Because of our
    split API setup with two different reverse proxies, the external
    and internal paths aren't the same, so we need the external path
    as well to be able to generate correct client-resolvable URLs.

    Args:
        request: The Falcon request to inspect.

    Return:
        A string containing the external URL requested.
    """
    ext_proto = request.get_header('X-Forwarded-Proto')
    ext_host = request.get_header('X-Forwarded-Host')
    ext_path = request.get_header('X-Forwarded-Path')

    if ext_proto and ext_host and ext_path:
        ext_path = quote(ext_path, safe='/')
        return f'{ext_proto}://{ext_host}{ext_path}'
    else:
        prefix = request.prefix
        path = quote(request.path, safe='/')
        return f'{request.prefix}{path}'


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
                    asset.image_location = _request_url(request) + '/image'

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
            self, request: Request, response: Response, asset_id: str) -> None:
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


class AssetConnectionAccessHandler:
    """A handler for the /assets/{assetId}/connect endpoints."""
    def __init__(self, store: IAssetStore) -> None:
        """Create an AssetConnectionAccessHandler handler.

        Args:
            store: The asset store to send requests to.
        """
        self._store = store

    def on_post(
            self, request: Request, response: Response, asset_id: str) -> None:
        """Handle request for a new asset connection.

        Args:
            request: The submitted request.
            response: A response object to configure.
            asset_id: The id of the requested asset
        """
        logger.info(f'Asset connection request, store = {self._store}')
        try:
            if 'requester' not in request.params:
                logger.info(f'Invalid asset access request')
                raise ValidationError('No requester specified')
            else:

                logger.info(
                        f'Received request to connect to asset {asset_id} from'
                        f' {request.params["requester"]}')
                validate_json('ConnectionRequest', request.media)
                conn_request = deserialize(ConnectionRequest, request.media)

                conn_info = self._store.serve(
                        Identifier(asset_id), conn_request,
                        request.params['requester'])

                response.status = HTTP_200
                response.media = serialize(conn_info)
        except KeyError:
            logger.info(f'Asset {asset_id} not found')
            response.status = HTTP_404
            response.body = 'Asset not found'
        except RuntimeError:
            # This is permission denied, but we return a 404 to
            # avoid information-leaking the existence of any
            # particular assets.
            logger.info(
                    f'Asset {asset_id} connection not available for user'
                    f' {request.params["requester"]}')
            response.status = HTTP_404
            response.body = 'Asset not found'
        except ValueError:
            # raised by Identifier(invalid_id)
            response.status = HTTP_400
            response.body = 'Invalid request'
        except ValidationError:
            response.status = HTTP_400
            response.body = 'Invalid request'
        # TODO: return 503 when connections are disabled altogether


class ConnectionsHandler:
    """A handler for the /connections/{connId} endpoint."""
    def __init__(self, store: IAssetStore) -> None:
        """Create an AssetDisconnectionHandler handler.

        Args:
            store: The asset store to send requests to.
        """
        self._store = store

    def on_delete(
            self, request: Request, response: Response, conn_id: str
            ) -> None:
        """Handle request for disconnecting from an asset.

        Args:
            request: The submitted request.
            response: A response object to configure.
            conn_id: The id of the connection to remove.
        """
        logger.info(f'Asset disconnection request, store = {self._store}')
        try:
            if 'requester' not in request.params:
                logger.info(f'Invalid asset access request')
                raise ValidationError('No requester specified')
            else:
                logger.info(
                        f'Received request to disconnect connection {conn_id}'
                        f' from {request.params["requester"]}')

                self._store.stop_serving(conn_id, request.params['requester'])
                response.status = HTTP_200
        except KeyError:
            logger.info(f'Connection {conn_id} not found')
            response.status = HTTP_404
            response.body = 'Connection not found'
        except RuntimeError:
            logger.info(
                    f'Connection {conn_id} not owned by user'
                    f' {request.params["requester"]}')
            response.status = HTTP_403
            response.body = 'Connection not yours'
        # TODO: return 503 when connections are disabled altogether


class AssetManagementHandler:
    """A handler for the internal /assets endpoint."""
    def __init__(self, store: IAssetStore) -> None:
        """Create an AssetManagementHandler handler.

        Args:
            store: The asset store to send requests to.

        """
        self._store = store

    def on_post(self, request: Request, response: Response) -> None:
        """Handle adding an asset.

        Args:
            request: The submitted request.
            response: A response object to configure.

        """
        try:
            logger.info(f'Asset storage request')
            validate_json('Asset', request.media)
            asset = deserialize(Asset, request.media)
            logger.info(f'Storing asset {asset}')
            self._store.store(asset)
            response.status = HTTP_201
            response.body = 'Created'
        except ValidationError:
            logger.warning(f'Invalid asset storage request: {request.media}')
            response.status = HTTP_400
            response.body = 'Invalid request'


class AssetImageManagementHandler:
    """A handler for the internal /assets/.../image endpoint."""

    _CHUNK_SIZE = 1024 * 1024

    def __init__(self, store: IAssetStore) -> None:
        """Create an AssetImageManagementHandler handler.

        Args:
            store: The asset store to send images to.

        """
        self._store = store

    def on_put(
            self, request: Request, response: Response, asset_id: str) -> None:
        """Handle adding an asset image.

        Args:
            request: The submitted request.
            response: A response object to configure.
            asset_id: ID of the asset to store an image for.

        """
        try:
            asset_id = Identifier(asset_id)
        except ValueError:
            logger.warning(f'Invalid asset image storage request')
            response.status = HTTP_400
            response.body = 'Invalid asset id'
            return

        with NamedTemporaryFile(delete=False) as f:
            chunk = request.bounded_stream.read(self._CHUNK_SIZE)
            while chunk:
                f.write(chunk)
                chunk = request.bounded_stream.read(self._CHUNK_SIZE)

            file_path = Path(f.name)

        try:
            self._store.store_image(asset_id, file_path, move_image=True)
            response.status = HTTP_201
            response.body = 'Created'
        except KeyError:
            logger.warning(f'Image storage requested for unknown asset')
            response.status = HTTP_404
            response.body = 'Unknown asset id'


class PolicyManagementHandler:
    """A handler for the internal /rules endpoint."""
    def __init__(self, policy_store: PolicyStore) -> None:
        """Create a PolicyManagementHandler handler.

        Args:
            policy_store: A policy store to store rules in.

        """
        self._policy_store = policy_store

    def on_post(self, request: Request, response: Response) -> None:
        """Handle request to add a rule.

        Args:
            request: The submitted request.
            response: A response object to configure.

        """
        try:
            validate_json('Rule', request.media)
            rule = deserialize(Rule, request.media)
            self._policy_store.insert(rule)
            response.status = HTTP_201
            response.body = 'Created'
        except ValidationError:
            logger.warning(f'Invalid rule submitted: {request.media}')
            response.status = HTTP_400
            response.body = 'Invalid request'


class WorkflowExecutionHandler:
    """A handler for the external /jobs endpoint."""
    def __init__(self, runner: IStepRunner) -> None:
        """Create a WorkflowExecutionHandler handler.

        Args:
            runner: The runner to send requests to.
        """
        self._runner = runner

    def on_post(self, request: Request, response: Response) -> None:
        """Handle request to execute part of a workflow.

        Args:
            request: The submitted request.
            response: A response object to configure.

        """
        try:
            logger.info(f'Received execution request: {request.media}')
            validate_json('ExecutionRequest', request.media)
            request = deserialize(ExecutionRequest, request.media)
            self._runner.execute_request(request)
            response.status = HTTP_201
            response.body = 'Created'
        except ValidationError:
            logger.warning(f'Invalid execution request: {request.media}')
            response.status = HTTP_400
            response.body = 'Invalid request'


class WorkflowSubmissionHandler:
    """A handler for the internal /jobs endpoint.

    This lets internal users submit jobs, which are identified by a
    unique URI.

    """
    def __init__(self, orchestrator: WorkflowOrchestrator) -> None:
        """Create a WorkflowSubmissionHandler handler.

        Args:
            orchestrator: The orchestrator to use to execute the
                    submitted workflows.

        """
        self._orchestrator = orchestrator

    def on_post(self, request: Request, response: Response) -> None:
        """Handle request to orchestrate a workflow.

        Args:
            request: The submitted request.
            response: A response object to configure.

        """
        if (
                'requesting_party' not in request.params or
                'requesting_site' not in request.params):
            logger.info(f'Invalid job submission')
            response.status = HTTP_400
            response.body = 'Invalid request'
            return

        requesting_party = request.params['requesting_party']
        requesting_site = request.params['requesting_site']

        try:
            validate_json('Job', request.media)
            job = deserialize(Job, request.media)
            logger.info(
                    f'Received new job {request.media} from'
                    f' {requesting_party}')
            job_id = self._orchestrator.start_job(
                    requesting_party, requesting_site, job)
            logger.info(f'Created new job {job_id} for {requesting_party}')
            response.status = HTTP_303
            response.location = _request_url(request) + '/' + job_id
        except ValidationError:
            logger.warning(f'Invalid execution request: {request.media}')
            response.status = HTTP_400
            response.body = 'Invalid request'


class WorkflowStatusHandler:
    """A handler for the internal /jobs/{job_id} endpoint.

    This lets internal users check up on their jobs and retrieve
    results.

    """
    def __init__(self, orchestrator: WorkflowOrchestrator) -> None:
        """Create a WorkflowStatusHandler handler.

        Args:
            orchestrator: The orchestrator to use to retrieve the
                    requested results from.

        """
        self._orchestrator = orchestrator

    def on_get(
            self, request: Request, response: Response, job_id: str) -> None:
        """Handle request for job status update.

        Args:
            request: The submitted request.
            response: A response object to configure.
            job_id: The orchestrator job id.

        """
        logger.debug(f'Handling request for status of job {job_id}')
        try:
            job = self._orchestrator.get_submitted_job(job_id)
            plan = self._orchestrator.get_plan(job_id)
            is_done = self._orchestrator.is_done(job_id)
        except KeyError:
            logger.warning(f'Request for non-existent job {job_id}')
            response.status = HTTP_404
            response.body = 'Job not found'
            return

        outputs = dict()    # type: Dict[str, Asset]
        if is_done:
            outputs = self._orchestrator.get_results(job_id)

        result = JobResult(job, plan, is_done, outputs)
        response.status = HTTP_200
        response.media = serialize(result)


class SiteRestApi:
    """The complete Site REST API.

    Attributes:
        app: The WSGI application object.

    """
    def __init__(
            self,
            policy_store: PolicyStore,
            asset_store: IAssetStore,
            runner: IStepRunner,
            orchestrator: WorkflowOrchestrator) -> None:
        """Create a SiteRestApi instance.

        Args:
            policy_store: The store to offer policy updates from.
            asset_store: The store to serve assets from.
            runner: The workflow runner to send requests to.
            orchestrator: The orchestrator to use to orchestrate
                    user job submissions.

        """
        self.app = App()

        rule_replication = ReplicationHandler[Rule](policy_store)
        self.app.add_route('/external/rules/updates', rule_replication)

        asset_access = AssetAccessHandler(asset_store)
        self.app.add_route('/external/assets/{asset_id}', asset_access)

        asset_image_access = AssetImageAccessHandler(asset_store)
        self.app.add_route(
                '/external/assets/{asset_id}/image', asset_image_access)

        asset_connection_access = AssetConnectionAccessHandler(asset_store)
        self.app.add_route(
                '/external/assets/{asset_id}/connect',
                asset_connection_access)

        connections = ConnectionsHandler(asset_store)
        self.app.add_route(
                '/external/connections/{conn_id}', connections)

        workflow_execution = WorkflowExecutionHandler(runner)
        self.app.add_route('/external/jobs', workflow_execution)

        asset_management = AssetManagementHandler(asset_store)
        self.app.add_route('/internal/assets', asset_management)

        asset_image_management = AssetImageManagementHandler(asset_store)
        self.app.add_route(
                '/internal/assets/{asset_id}/image', asset_image_management)

        policy_management = PolicyManagementHandler(policy_store)
        self.app.add_route('/internal/rules', policy_management)

        workflow_status = WorkflowStatusHandler(orchestrator)
        self.app.add_route('/internal/jobs/{job_id}', workflow_status)

        workflow_submission = WorkflowSubmissionHandler(orchestrator)
        self.app.add_route('/internal/jobs', workflow_submission)


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

        self.internal_endpoint = (
                f'http://{self._server.server_name}'
                f':{self._server.server_port}/internal')
        logger.info(f'Site server listening on {self.internal_endpoint}')

    def close(self) -> None:
        """Stop the server thread."""
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()


def wsgi_app() -> App:
    """Creates a WSGI app for a WSGI runner."""
    settings = load_settings()

    logging.basicConfig(level=settings.loglevel.upper())

    registry_rest_client = RegistryRestClient(
            settings.registry_endpoint, settings.trust_store)
    registry_client = RegistryClient(registry_rest_client)
    site = Site(settings, [], [], registry_client)
    return SiteRestApi(
            site.policy_store, site.store, site.runner, site.orchestrator).app
