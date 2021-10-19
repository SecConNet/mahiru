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
