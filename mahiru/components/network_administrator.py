"""Local network management to support step execution."""

import docker
import logging
from pathlib import Path
from threading import Lock
from typing import cast, Dict, Tuple, Optional

from mahiru.components.settings import NetworkSettings
from mahiru.definitions.assets import Asset
from mahiru.definitions.connections import (
        ConnectionInfo, ConnectionRequest, WireGuardConnectionInfo,
        WireGuardConnectionRequest, WireGuardEndpoint)
from mahiru.definitions.interfaces import INetworkAdministrator
from mahiru.rest.site_client import SiteRestClient


_WG_CLIENT_HOST = 0
_WG_SERVER_HOST = 1
_NAH_IMAGE = 'net-admin-helper:latest'


logger = logging.getLogger(__name__)


class WireGuardNA(INetworkAdministrator):
    """Manages plain WireGuard connections to remote containers.

    This uses one overlay network per container pair to set up the
    communication. Requires the net-admin-helper Docker image to be
    available in the local Docker daemon.
    """
    def __init__(
            self, settings: NetworkSettings, site_rest_client: SiteRestClient
            ) -> None:
        """Create a WGNetworkAdministrator.

        Args:
            settings: Settings to use for making connections.
            site_rest_client: Client for talking to other sites.
        """
        self._settings = settings
        self._site_rest_client = site_rest_client

        if settings.ports is not None:
            self._available_ports = set(range(
                    settings.ports[0], settings.ports[1] + 1))

        # conn_id for each job_id, input_name pair
        self._active_connections = dict()   # type: Dict[int, Dict[str, str]]

        self._served_lock = Lock()          # protects the below
        self._served_ports = dict()         # type: Dict[str, int]
        self._served_nets = dict()          # type: Dict[str, int]

        # Hardcoded for now, but must be passed in from the DA if we're
        # running containers in another VM.
        self._dcli = docker.from_env()

    def serve_asset(
            self, conn_id: str, network_namespace: int,
            request: ConnectionRequest) -> WireGuardConnectionInfo:
        """Create a public endpoint to serve an asset.

        Args:
            conn_id: Connection id for this connection.
            network_namespace: PID of the network namespace to create
                    the endpoint inside of.
            request: Connection request from the client side.

        Return:
            A connection info object to send back to the client.
        """
        if not isinstance(request, WireGuardConnectionRequest):
            raise RuntimeError("Request type not supported")

        local_endpoint = self._create_wg_endpoint(
                network_namespace, request.net, _WG_SERVER_HOST)

        self._connect_wg_endpoint(
                network_namespace, request.net, _WG_SERVER_HOST,
                request.endpoint)

        with self._served_lock:
            self._served_nets[conn_id] = request.net
            self._served_ports[conn_id] = local_endpoint.port

        local_conn_info = WireGuardConnectionInfo(conn_id, local_endpoint)
        return local_conn_info

    def stop_serving_asset(
            self, conn_id: str, network_namespace: int) -> None:
        """Remove a public endpoint and free resources.

        Args:
            conn_id: Id of the connection to stop.
            network_namespace: PID of the network namespace the
                    endpoint was created inside of.
        """
        if not self._settings.enabled:
            raise RuntimeError('Asset connections are disabled')

        self._remove_wg_endpoint(
                network_namespace, self._served_nets[conn_id], _WG_SERVER_HOST)

        with self._served_lock:
            del self._served_ports[conn_id]

    def connect_to_inputs(
            self, job_id: int, inputs: Dict[str, Asset],
            network_namespace: int
            ) -> Tuple[Dict[str, str], Dict[str, Asset]]:
        """Connect a local network namespace to a set of inputs.

        Args:
            job_id: Job id for future reference.
            inputs: Assets to connect to, indexed by input name.
            network_namespace: Namespace to create network interfaces
                    inside of.

        Return:
            (nets, remaining) where `nets` is a dictionary indexed by
            input name which contains a host IP for each
            successfully connected input, and `remaining` contains the
            assets from inputs that we could not connect to, also
            indexed by input name.
        """
        nets = dict()           # type: Dict[str, str]
        remaining = dict()      # type: Dict[str, Asset]
        conn_info = None        # type: Optional[ConnectionInfo]

        if not self._settings.enabled:
            return nets, inputs

        self._active_connections[job_id] = dict()

        for net, (name, asset) in enumerate(inputs.items()):
            logger.debug(f'Trying to connect for {name} on {net}')
            try:
                endpoint = self._create_wg_endpoint(
                        network_namespace, net, _WG_CLIENT_HOST)
            except Exception as e:
                logger.debug(f'Failed to create endpoint: {e}')
                remaining[name] = asset
                continue

            try:
                request = WireGuardConnectionRequest(net, endpoint)
                conn_info = self._site_rest_client.connect_to_asset(
                        asset.id, request)
                if not isinstance(conn_info, WireGuardConnectionInfo):
                    raise RuntimeError('Peer incompatible')
                logger.debug(f'Connection request successful')
                self._connect_wg_endpoint(
                        network_namespace, net, _WG_CLIENT_HOST,
                        conn_info.endpoint)
                self._active_connections[job_id][name] = conn_info.conn_id
                nets[name] = self._net_to_addr(net, _WG_SERVER_HOST)
                logger.debug(f'Connected')
            except Exception as e:
                logger.error(f'Error completing connection: {e}')
                if conn_info:
                    self._site_rest_client.disconnect_asset(
                            asset.id, conn_info.conn_id)
                self._remove_wg_endpoint(
                        network_namespace, net, _WG_CLIENT_HOST)
                remaining[name] = asset

        return nets, remaining

    def disconnect_inputs(self, job_id: int, inputs: Dict[str, Asset]) -> None:
        """Disconnect inputs and free resources.

        Args:
            job_id: Job id of job to disconnect.
            inputs: Input assets for this job, indexed by input name.
        """
        for name, conn_id in self._active_connections[job_id].items():
            try:
                self._site_rest_client.disconnect_asset(
                        inputs[name].id, conn_id)
            except Exception as e:
                logger.warning('Could not disconnect input: {e}')
                # ignore, we tried our best

    def _create_wg_endpoint(
            self, pid: int, net: int, host: int) -> WireGuardEndpoint:
        """Create a local WireGuard endpoint.

        This will create an endpoint in the network namespace
        with the given PID, for the given net and host.

        Args:
            pid: PID of the network namespace (container) to create the
                endpoint inside of.
            net: Network to create the endpoint for.
            host: Host id, either _SERVER_HOST or _CLIENT_HOST.

        Return:
            The local endpoint for the remote side to connect to.
        """
        try:
            our_port = self._available_ports.pop()
        except KeyError:
            raise RuntimeError('Insufficient resources')

        try:
            our_key = self._run_net_admin_helper(
                    f'cwg_create {pid} {net} {host} {our_port}')
        except Exception:
            self._available_ports.add(our_port)
            raise

        our_ip = self._settings.external_ip
        assert our_ip is not None
        return WireGuardEndpoint(our_ip, our_port, our_key)

    def _connect_wg_endpoint(
            self, pid: int, net: int, host: int, endpoint: WireGuardEndpoint
            ) -> None:
        """Connect a local WireGuard endpoint to a remote one.

        Args:
            pid: PID of local namespace the endpoint is in.
            net: The network the local endpoint (and hopefully the
                    remote one too!) is for.
            host: The local host id (either _SERVER_HOST or
                    _CLIENT_HOST)
            endpoint: The remote endpoint to connect to.
        """
        self._run_net_admin_helper(
                f'cwg_connect {pid} {net} {host} {endpoint.endpoint()}'
                f' {endpoint.key}')

    def _remove_wg_endpoint(
            self, pid: int, net: int, host: int) -> None:
        """Remove a local WireGuard endpoint.

        Args:
            pid: PID of local namespace the endpoint is in.
            net: The network the local endpoint is for.
            host: The local host id (either _SERVER_HOST or
                    _CLIENT_HOST)
        """
        self._run_net_admin_helper(f'cwg_destroy {pid} {net} {host}')

    def _ensure_net_admin_helper(self) -> None:
        """Ensures that the NAH Docker image is loaded into Docker."""
        try:
            self._dcli.images.get(_NAH_IMAGE)
        except docker.errors.ImageNotFound:
            image_file = (
                    Path(__file__).parents[1] / 'data' /
                    'net-admin-helper.tar.gz')
            with image_file.open('rb') as f:
                image = self._dcli.images.load(f.read())[0]

    def _run_net_admin_helper(self, command: str) -> str:
        """Run net-admin-helper and return its output.

        Args:
            command: The subcommand to run, including arguments.
        """
        logger.debug(f'Running net-admin-helper {command}')
        self._ensure_net_admin_helper()
        container = self._dcli.containers.run(
                _NAH_IMAGE, f'/usr/local/bin/net-admin-helper {command}',
                cap_add=[
                    'NET_ADMIN', 'SYS_ADMIN', 'SYS_PTRACE', 'IPC_LOCK'],
                network_mode='host', pid_mode='host', detach=True)

        result = container.wait()

        if result['StatusCode'] != 0:
            error = container.logs().decode('utf-8')
            container.remove(force=True)
            raise RuntimeError(f'net-admin-helper: {error}')

        logs = cast(bytes, container.logs(stdout=True, stderr=False))
        container.remove(force=True)
        return logs.decode('ascii').strip()

    def _net_to_addr(self, net: int, host: int) -> str:
        """Return the IP for a given network number and host.

        We use net-admin-helper to set up connections, and
        net-admin-helper uses net numbers to specify the IP addresses
        used by the endpoints of the WireGuard link. The net number
        is an integer containing bits 1:24 of the IP addresses used.
        The least-significant bit is 0 for one host and 1 for the
        other, as specified by the host argument, so the networks are
        all /31 networks. The top 8 bits are set to decimal 10, to put
        us in the 10.x.y.z IPV4 private address range.
        """
        ip = (10 << 24) | ((net & 0x7fffff) << 1) | (host & 1)
        b0 = ip & 0xff
        b1 = (ip >> 8) & 0xff
        b2 = (ip >> 16) & 0xff
        b3 = (ip >> 24) & 0xff
        return f'{b3}.{b2}.{b1}.{b0}'
