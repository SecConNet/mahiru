"""Local network management to support step execution."""

import logging
from typing import Dict, Tuple

from mahiru.components.settings import NetworkSettings
from mahiru.definitions.assets import Asset
from mahiru.definitions.connections import ConnectionInfo, ConnectionRequest
from mahiru.definitions.interfaces import INetworkAdministrator
from mahiru.rest.site_client import SiteRestClient


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

    def serve_asset(
            self, conn_id: str, network_namespace: int,
            request: ConnectionRequest) -> ConnectionInfo:
        """Create a public endpoint to serve an asset.

        Args:
            conn_id: Connection id for this connection.
            network_namespace: PID of the network namespace to create
                    the endpoint inside of.
            request: Connection request from the client side.

        Return:
            A connection info object to send back to the client.
        """
        raise NotImplementedError()

    def stop_serving_asset(
            self, conn_id: str, network_namespace: int) -> None:
        """Remove a public endpoint and free resources.

        Args:
            conn_id: Id of the connection to stop.
            network_namespace: PID of the network namespace the
                    endpoint was created inside of.
        """
        raise NotImplementedError()

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
        raise NotImplementedError()

    def disconnect_inputs(self, job_id: int, inputs: Dict[str, Asset]) -> None:
        """Disconnect inputs and free resources.

        Args:
            job_id: Job id of job to disconnect.
            inputs: Input assets for this job, indexed by input name.
        """
        raise NotImplementedError()
