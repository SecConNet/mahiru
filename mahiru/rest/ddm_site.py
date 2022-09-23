"""REST-style API for a site."""
from copy import copy
from enum import Enum
import logging
from pathlib import Path
from socketserver import ThreadingMixIn
from tempfile import NamedTemporaryFile
from threading import Thread
from typing import Dict, List
from urllib.parse import quote, unquote_to_bytes
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer

from cryptography.hazmat.primitives.serialization import Encoding
from cryptography import x509
from cryptography.x509 import Certificate
from cryptography.x509.oid import ExtensionOID
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


class InternalOperation(Enum):
    """Operation on the internal API.

    This enumerates the different kinds of operations that can be
    performed by local users through the internal API. It's intended
    to model the relevant operations for a Role-Based Access Control
    system.
    """
    MANAGE_ASSETS = 1
    MANAGE_POLICIES = 2
    SUBMIT_WORKFLOWS = 3


class AccessController:
    """Performs access control for the internal REST API.

    For now, we just check whether the user presented a valid
    certificate that identifies them as a user affiliated with the
    owner of this site. Some kind of RBAC should be implemented in a
    production version.
    """
    def __init__(
            self, registry_client: RegistryClient, owner: Identifier) -> None:
        """Create an AccessController.

        Args:
            registry_client: Registry client to use to check
                certificates.
            owner: The owner of this site.
        """
        self._registry_client = registry_client
        self._owner = owner

    def check_requester(
            self, requester: Identifier, client_cert: Certificate) -> None:
        """Checks whether the requester is who they say they are.

        Used by the external API endpoints to validate the site making
        the request.

        This checks the value of the 'requester' parameter against the
        identity of the client as specified in their HTTPS certificate.
        If the endpoint specified by the latter is indeed the endpoint
        of the site specified by the former, then the request comes
        from where we think it comes, and we can return the asset if
        that site has permission to have it.

        Args:
            requester: The claimed requester.
            client_cert: The client's HTTPS certificate.
        """
        logger.info(f'Requester cert: {client_cert}')
        subj_alt_name_ext = client_cert.extensions.get_extension_for_oid(
                ExtensionOID.SUBJECT_ALTERNATIVE_NAME)
        client_dns_names = subj_alt_name_ext.value.get_values_for_type(
                x509.DNSName)
        if len(client_dns_names) != 1:
            raise RuntimeError(
                    'Client certificate has more than one subjAltName')
        client_domain = client_dns_names[0]
        logger.debug(f'Client domain from certificate: {client_domain}')

        try:
            self._registry_client.update()
            site_desc = self._registry_client.get_site_by_id(requester)
        except Exception as e:
            logger.error(f'Invalid requester {requester}: {e}')
            raise RuntimeError('Invalid requester')

        logger.debug(f'Site endpoint: {site_desc.endpoint}')
        site_domain = site_desc.endpoint.split('://')[1]
        logger.debug(f'Site domain from registry: {site_domain}')
        if client_domain != site_domain:
            raise RuntimeError(
                    f'Request claims to be from {requester} which has domain'
                    f' {site_domain}, but came from {client_domain}')

    def check_user_authorization(
            self, client_cert: bytes, operation: InternalOperation) -> None:
        """Checks that the client is authorised.

        Used by the internal API endpoints to validate the user making
        the request.

        This function would implement a role-based access mechanism to
        check that the user is authorised to perform an action. Since
        those are well-proven, we don't implement any in this proof-
        of-concept.

        Args:
            client_cert: The client's certificate as presented to the
                HTTPS server.
            operation: The operation the client is asking to perform.
        """
        pass

    def _load_certs(self, cert_bytes: bytes) -> List[Certificate]:
        """Load a list of certificates from a byte stream.

        This loads zero or more X.509 certificates from a byte
        stream as typically sent to an HTTPS server.

        Args:
            cert_bytes: The certificate data.

        Return:
            A list of parsed certificates.

        Raises:
            RuntimeError: If there was a problem parsing the data.
        """
        lines = cert_bytes.decode('ascii').splitlines()
        logger.debug(f'lines: {lines}')
        cert_list = list()      # type: List[List[str]]
        state = 'before cert'
        for line in lines:
            logger.debug(f'cert list: {cert_list}')
            logger.debug(f'state: "{state}"')
            logger.debug(f'line: "{line}"')
            if line.split() == []:      # skip whitespace only lines
                continue

            if state == 'before cert':
                if line != '-----BEGIN CERTIFICATE-----':
                    raise RuntimeError('Could not parse certificate')
                cert_list.append([line])    # lines for this cert
                state = 'in cert'
            elif state == 'in cert':
                cert_list[-1].append(line)
                if line == '-----END CERTIFICATE-----':
                    state = 'before cert'

        if state != 'before cert':
            raise RuntimeError('Could not parse certificate')

        return [
                x509.load_pem_x509_certificate(
                    '\n'.join(cert_lines).encode('ascii'))
                for cert_lines in cert_list]


class AssetAccessHandler:
    """A handler for the external /assets endpoint."""
    def __init__(
            self, access_controller: AccessController, store: IAssetStore
            ) -> None:
        """Create an AssetAccessHandler handler.

        Args:
            access_controller: Access controller to use.
            store: The asset store to send requests to.
        """
        self._access_controller = access_controller
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
        logger.info(f'Asset access request')
        if 'requester' not in request.params:
            logger.info(f'Invalid asset access request')
            response.status = HTTP_400
            response.body = 'Invalid request'
        else:
            logger.info(
                    f'Received request for asset {asset_id} from'
                    f' {request.params["requester"]}')
            try:
                requester = Identifier(request.params['requester'])
                client_cert_header = request.get_header('X-Client-Certificate')
                if client_cert_header:
                    client_cert = x509.load_pem_x509_certificate(
                        unquote_to_bytes(client_cert_header))
                    self._access_controller.check_requester(
                            requester, client_cert)

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
    def __init__(
            self, access_controller: AccessController, store: IAssetStore
            ) -> None:
        """Create an AssetImageAccessHandler handler.

        Args:
            access_controller: Access controller to use.
            store: The asset store to send requests to.
        """
        self._access_controller = access_controller
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
            requester = Identifier(request.params['requester'])
            client_cert_header = request.get_header('X-Client-Certificate')
            if client_cert_header:
                client_cert = x509.load_pem_x509_certificate(
                    unquote_to_bytes(client_cert_header))
                self._access_controller.check_requester(requester, client_cert)

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
    def __init__(
            self, access_controller: AccessController, store: IAssetStore
            ) -> None:
        """Create an AssetConnectionAccessHandler handler.

        Args:
            access_controller: Access controller to use.
            store: The asset store to send requests to.
        """
        self._access_controller = access_controller
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

                requester = Identifier(request.params['requester'])
                client_cert_header = request.get_header('X-Client-Certificate')
                if client_cert_header:
                    client_cert = x509.load_pem_x509_certificate(
                        unquote_to_bytes(client_cert_header))
                    self._access_controller.check_requester(
                            requester, client_cert)

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
    def __init__(
            self, access_controller: AccessController, store: IAssetStore
            ) -> None:
        """Create an AssetDisconnectionHandler handler.

        Args:
            access_controller: Access controller to use.
            store: The asset store to send requests to.
        """
        self._access_controller = access_controller
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
                requester = Identifier(request.params['requester'])
                client_cert_header = request.get_header('X-Client-Certificate')
                if client_cert_header:
                    client_cert = x509.load_pem_x509_certificate(
                        unquote_to_bytes(client_cert_header))
                    self._access_controller.check_requester(
                            requester, client_cert)

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
    def __init__(
            self, access_controller: AccessController, store: IAssetStore
            ) -> None:
        """Create an AssetManagementHandler handler.

        Args:
            access_controller: Access controller to use.
            store: The asset store to send requests to.

        """
        self._access_controller = access_controller
        self._store = store

    def on_post(self, request: Request, response: Response) -> None:
        """Handle adding an asset.

        Args:
            request: The submitted request.
            response: A response object to configure.

        """
        try:
            logger.info(f'Asset storage request')
            client_cert_header = request.get_header('X-Client-Certificate')
            if client_cert_header:
                self._access_controller.check_user_authorization(
                        unquote_to_bytes(client_cert_header),
                        InternalOperation.MANAGE_ASSETS)
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

    def __init__(
            self, access_controller: AccessController, store: IAssetStore
            ) -> None:
        """Create an AssetImageManagementHandler handler.

        Args:
            access_controller: Access controller to use.
            store: The asset store to send images to.

        """
        self._access_controller = access_controller
        self._store = store

    def on_put(
            self, request: Request, response: Response, asset_id: str) -> None:
        """Handle adding an asset image.

        Args:
            request: The submitted request.
            response: A response object to configure.
            asset_id: ID of the asset to store an image for.

        """
        client_cert_header = request.get_header('X-Client-Certificate')
        if client_cert_header:
            self._access_controller.check_user_authorization(
                    unquote_to_bytes(client_cert_header),
                    InternalOperation.MANAGE_ASSETS)

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
    def __init__(
            self, access_controller: AccessController,
            policy_store: PolicyStore) -> None:
        """Create a PolicyManagementHandler handler.

        Args:
            access_controller: Access controller to use.
            policy_store: A policy store to store rules in.

        """
        self._access_controller = access_controller
        self._policy_store = policy_store

    def on_post(self, request: Request, response: Response) -> None:
        """Handle request to add a rule.

        Args:
            request: The submitted request.
            response: A response object to configure.

        """
        try:
            client_cert_header = request.get_header('X-Client-Certificate')
            if client_cert_header:
                self._access_controller.check_user_authorization(
                        unquote_to_bytes(client_cert_header),
                        InternalOperation.MANAGE_POLICIES)

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
    def __init__(
            self, access_controller: AccessController,
            orchestrator: WorkflowOrchestrator) -> None:
        """Create a WorkflowSubmissionHandler handler.

        Args:
            access_controller: Access controller to use.
            orchestrator: The orchestrator to use to execute the
                    submitted workflows.

        """
        self._access_controller = access_controller
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
            client_cert_header = request.get_header('X-Client-Certificate')
            if client_cert_header:
                self._access_controller.check_user_authorization(
                        unquote_to_bytes(client_cert_header),
                        InternalOperation.SUBMIT_WORKFLOWS)
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
    def __init__(
            self, access_controller: AccessController,
            orchestrator: WorkflowOrchestrator) -> None:
        """Create a WorkflowStatusHandler handler.

        Args:
            access_controller: Access controller to use.
            orchestrator: The orchestrator to use to retrieve the
                    requested results from.

        """
        self._access_controller = access_controller
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

        client_cert_header = request.get_header('X-Client-Certificate')
        if client_cert_header:
            self._access_controller.check_user_authorization(
                    unquote_to_bytes(client_cert_header),
                    InternalOperation.SUBMIT_WORKFLOWS)
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
            access_controller: AccessController,
            policy_store: PolicyStore,
            asset_store: IAssetStore,
            runner: IStepRunner,
            orchestrator: WorkflowOrchestrator) -> None:
        """Create a SiteRestApi instance.

        Args:
            access_controller: Access controller to use.
            policy_store: The store to offer policy updates from.
            asset_store: The store to serve assets from.
            runner: The workflow runner to send requests to.
            orchestrator: The orchestrator to use to orchestrate
                    user job submissions.

        """
        self.app = App()

        rule_replication = ReplicationHandler[Rule](policy_store)
        self.app.add_route('/external/rules/updates', rule_replication)

        asset_access = AssetAccessHandler(access_controller, asset_store)
        self.app.add_route('/external/assets/{asset_id}', asset_access)

        asset_image_access = AssetImageAccessHandler(
                access_controller, asset_store)
        self.app.add_route(
                '/external/assets/{asset_id}/image', asset_image_access)

        asset_connection_access = AssetConnectionAccessHandler(
                access_controller, asset_store)
        self.app.add_route(
                '/external/assets/{asset_id}/connect',
                asset_connection_access)

        connections = ConnectionsHandler(access_controller, asset_store)
        self.app.add_route(
                '/external/connections/{conn_id}', connections)

        workflow_execution = WorkflowExecutionHandler(runner)
        self.app.add_route('/external/jobs', workflow_execution)

        asset_management = AssetManagementHandler(
                access_controller, asset_store)
        self.app.add_route('/internal/assets', asset_management)

        asset_image_management = AssetImageManagementHandler(
                access_controller, asset_store)
        self.app.add_route(
                '/internal/assets/{asset_id}/image', asset_image_management)

        policy_management = PolicyManagementHandler(
                access_controller, policy_store)
        self.app.add_route('/internal/rules', policy_management)

        workflow_status = WorkflowStatusHandler(
                access_controller, orchestrator)
        self.app.add_route('/internal/jobs/{job_id}', workflow_status)

        workflow_submission = WorkflowSubmissionHandler(
                access_controller, orchestrator)
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
            settings.registry_endpoint, settings.trust_store,
            settings.client_creds())
    registry_client = RegistryClient(registry_rest_client)
    access_controller = AccessController(registry_client, settings.owner)
    site = Site(settings, [], [], registry_client)
    return SiteRestApi(
            access_controller, site.policy_store, site.store, site.runner,
            site.orchestrator).app
