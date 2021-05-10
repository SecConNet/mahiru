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
        App, HTTP_200, HTTP_201, HTTP_204, HTTP_303, HTTP_400, HTTP_404,
        Request, Response)
import ruamel.yaml as yaml
import yatiml

from proof_of_concept.components.ddm_site import Site
from proof_of_concept.components.registry_client import RegistryClient
from proof_of_concept.components.orchestration import WorkflowOrchestrator
from proof_of_concept.definitions.assets import Asset
from proof_of_concept.definitions.execution import JobResult
from proof_of_concept.definitions.identifier import Identifier
from proof_of_concept.definitions.interfaces import IAssetStore, IStepRunner
from proof_of_concept.definitions.policy import Rule
from proof_of_concept.definitions.workflows import ExecutionRequest, Job
from proof_of_concept.policy.replication import PolicyStore
from proof_of_concept.rest.registry_client import RegistryRestClient
from proof_of_concept.rest.replication import ReplicationHandler
from proof_of_concept.rest.serialization import deserialize, serialize
from proof_of_concept.rest.validation import validate_json, ValidationError


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
        if 'requester' not in request.params:
            logger.info(f'Invalid job submission')
            response.status = HTTP_400
            response.body = 'Invalid request'
            return

        requester = request.params['requester']

        try:
            validate_json('Job', request.media)
            job = deserialize(Job, request.media)
            job_id = self._orchestrator.start_job(requester, job)
            logger.info(f'Received new job {job_id} from {requester}')
            response.status = HTTP_303
            response.location = (
                    f'{request.forwarded_prefix}/internal/jobs/{job_id}')
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

    registry_rest_client = RegistryRestClient(settings.registry_endpoint)
    registry_client = RegistryClient(registry_rest_client)
    site = Site(
            settings.name, settings.owner, settings.namespace, [], [],
            registry_client)
    return SiteRestApi(
            site.policy_store, site.store, site.runner, site.orchestrator).app
