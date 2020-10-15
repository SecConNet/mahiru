"""REST API handlers/clients for the replication system."""
import logging
import requests
from typing import Any, Dict, Generic, Optional, Type, TypeVar

from falcon import Request, Response

from proof_of_concept.definitions import (
        IReplicationSource, ReplicaUpdate)
from proof_of_concept.replication import ReplicableArchive, ReplicationServer
from proof_of_concept.serialization import (
        serialize, deserialize_replica_update)
from proof_of_concept.validation import Validator


logger = logging.getLogger(__name__)


T = TypeVar('T')


class ReplicationHandler(Generic[T]):
    """A handler for a /updates REST API endpoint."""
    def __init__(self, server: ReplicationServer[T]) -> None:
        """Create a Replication handler.

        Args:
            server: The server to send requests to.
        """
        self._server = server

    def on_get(self, request: Request, response: Response) -> None:
        """Handle a registry update request.

        Args:
            request: The submitted request.
            response: A response object to configure.
        """
        if 'from_version' not in request.params:
            from_version = None     # type: Optional[int]
        else:
            from_version = request.get_param_as_int(
                    'from_version', required=True)

        updates = self._server.get_updates_since(from_version)
        response.media = serialize(updates)


class ReplicationClient(IReplicationSource[T]):
    """Client for a ReplicationHandler REST endpoint."""
    def __init__(
            self, endpoint: str,
            validator: Validator, update_type_tag: str, content_type_tag: str
            ) -> None:
        """Create a ReplicationClient.

        Note that replicated_type must match T, one is used by the
        type checker, the other is available at runtime to help us
        deserialize the correct type.

        Args:
            endpoint: URL of the endpoint to connect to.
            validator: Validator to use to validate incoming updates.
            update_type_tag: Name of schema type to use to validate
                update messages.
            content_type_tag: Tag of type to deserialize replicated
                objects with.
        """
        self._endpoint = endpoint
        self._validator = validator
        self._update_type_tag = update_type_tag
        self._content_type_tag = content_type_tag

    def get_updates_since(
            self, from_version: Optional[int]) -> ReplicaUpdate[T]:
        """Get updates since the given version.

        Args:
            from_version: Version to start at, None to get all updates.
        """
        params = dict()     # type: Dict[str, int]
        if from_version is not None:
            params['from_version'] = from_version
        r = requests.get(self._endpoint, params=params)
        update_json = r.json()
        logger.info(f'Replication update: {update_json}')
        self._validator.validate(self._update_type_tag, update_json)
        logger.info(f'Validated against {self._update_type_tag}')
        return deserialize_replica_update(self._content_type_tag, update_json)
