"""Definitions supporting direct container connections."""


class ConnectionRequest:
    """Base class for connection requests.

    This is a request sent from one site to another to connect to an
    asset remotely.
    """
    pass


class ConnectionInfo:
    """Base class for connection information.

    This is the response to a connection request.
    """
    conn_id: str


class WireGuardEndpoint:
    """Describes a WireGuard endpoint."""
    def __init__(
            self, address: str, port: int, key: str
            ) -> None:
        """Create a WireGuard endpoint description.

        Args:
            address: Public IP address of the endpoint.
            port: Public port number of the endpoint.
            key: Public key of the endpoint.
        """
        self.address = address
        self.port = port
        self.key = key

    def endpoint(self) -> str:
        """Returns the endpoint's address:port string."""
        return f'{self.address}:{self.port}'


class WireGuardConnectionRequest(ConnectionRequest):
    """Describes a request to provide a connection to a data source.

    There are always two sides to a connection, one which initiates
    connection setup and one which responds. The initiator is the
    client side of the REST request, and the container on that side
    contains a compute asset which is the client side of the connection
    within the VPN tunnel. The responder hosts the REST API for setting
    up connections, and the container on that side contains a data
    asset which is the server side of the connection within the VPN
    tunnel.
    """
    def __init__(self, net: int, endpoint: WireGuardEndpoint) -> None:
        """Create a ConnectionRequest.

        The network number passed here sets the IPv4 addresses to
        be used by the connection. If you're setting up multiple
        connections for the same container, then the network numbers
        must be different or we'll end up with different remote sides
        with the same IP. That would seriously complicate routing.

        Args:
            net: The network to use, in range [0, 8388607].
            endpoint: The requesting-side WireGuard endpoint to use.
        """
        self.net = net
        self.endpoint = endpoint


class WireGuardConnectionInfo(ConnectionInfo):
    """Describes a connection and its server-side endpoint.

    This is the response to a ConnectionRequest. It has a connection
    id, through which the initiator can reference the connection later,
    and the endpoint on the responder/server side.
    """
    def __init__(self, conn_id: str, endpoint: WireGuardEndpoint) -> None:
        """Create a ConnectionInfo.

        Args:
            conn_id: The connection id, a string of ASCII letters and
                    digits.
            endpoint: The requested-side (server-side) WireGuard
                    endpoint to use.
        """
        self.conn_id = conn_id
        self.endpoint = endpoint
