"""REST-style API for a site."""
import logging
from pathlib import Path
from threading import Thread
from socketserver import ThreadingMixIn
from typing import Any
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer

from falcon import App, HTTP_200, HTTP_400, HTTP_404, Request, Response
import ruamel.yaml as yaml

from proof_of_concept.definitions import (
        IAssetStore, ILocalWorkflowRunner, JobSubmission)
from proof_of_concept.policy import Rule
from proof_of_concept.policy_replication import PolicyServer
from proof_of_concept.replication_rest import ReplicationHandler
from proof_of_concept.serialization import deserialize, serialize
from proof_of_concept.validation import Validator, ValidationError


logger = logging.getLogger(__name__)


class AssetAccess:
    """A handler for the /assets endpoint."""
    def __init__(self, store: IAssetStore) -> None:
        """Create an AssetAccess handler.

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
                asset = self._store.retrieve(
                        asset_id, request.params['requester'])
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


class WorkflowExecution:
    """A handler for the /jobs endpoint."""
    def __init__(
            self, runner: ILocalWorkflowRunner, validator: Validator
            ) -> None:
        """Create a WorkflowExecution handler.

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


class SiteApi:
    """The complete Site REST API.

    Attributes:
        app: The WSGI application object.

    """
    def __init__(
            self,
            policy_server: PolicyServer,
            asset_store: IAssetStore,
            runner: ILocalWorkflowRunner) -> None:
        """Create a RegistryApi instance.

        Args:
            policy_server: The server to offer updates from.
            asset_store: The store to serve assets from.
            runner: The workflow runner to send requests to.

        """
        self.app = App()

        site_api_file = Path(__file__).parent / 'site_api.yaml'
        with open(site_api_file, 'r') as f:
            site_api_def = yaml.safe_load(f.read())

        validator = Validator(site_api_def)

        rule_replication = ReplicationHandler[Rule](policy_server)
        self.app.add_route('/rules/updates', rule_replication)

        asset_access = AssetAccess(asset_store)
        self.app.add_route('/assets/{asset_id}', asset_access)

        workflow_execution = WorkflowExecution(runner, validator)
        self.app.add_route('/jobs', workflow_execution)


class ThreadingWSGIServer (ThreadingMixIn, WSGIServer):
    """Threading version of a simple WSGI server."""
    pass


class SiteServer:
    """An HTTP server serving a SiteApi.

    Make sure to call `close()` when you're done, or the program will
    not shut down because the background thread will still be running.

    Attributes:
        endpoint: The HTTP endpoint at which the server can be reached.

    """
    def __init__(self, api: SiteApi) -> None:
        """Create a SiteServer serving an API.

        This starts a background thread with an HTTP server.

        Args:
            api: The API to serve.

        """
        self._server = ThreadingWSGIServer(('0.0.0.0', 0), WSGIRequestHandler)
        self._server.set_app(api.app)

        self._thread = Thread(
                target=self._server.serve_forever,
                name='RegistryServer')
        self._thread.start()

        self.endpoint = (
                f'http://{self._server.server_name}'
                f':{self._server.server_port}')
        logger.info(f'Site server listening on {self.endpoint}')

    def close(self) -> None:
        """Stop the server thread."""
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()
