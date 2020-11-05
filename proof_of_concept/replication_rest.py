"""REST API handlers/clients for the replication system."""
import logging
import requests
from typing import Any, Dict, Generic, Optional, Type, TypeVar

from falcon import Request, Response

from proof_of_concept.definitions import (
        IReplicationSource, ReplicaUpdate)
from proof_of_concept.replication import ReplicableArchive
from proof_of_concept.serialization import serialize, deserialize
from proof_of_concept.validation import Validator


logger = logging.getLogger(__name__)


T = TypeVar('T')


class ReplicationHandler(Generic[T]):
    """A handler for a /updates REST API endpoint."""
    def __init__(self, source: IReplicationSource[T]) -> None:
        """Create a Replication handler.

        Args:
            source: The source to get updates from.
        """
        self._source = source

    def on_get(self, request: Request, response: Response) -> None:
        """Handle a registry update request.

        Args:
            request: The submitted request.
            response: A response object to configure.
        """
        from_version = request.get_param_as_int(
                'from_version', required=True)

        updates = self._source.get_updates_since(from_version)
        response.media = serialize(updates)


class ReplicationClient(IReplicationSource[T]):
    """Client for a ReplicationHandler REST endpoint."""
    UpdateType = ReplicaUpdate[T]   # type: Type[ReplicaUpdate[T]]

    def __init__(self, endpoint: str, validator: Validator) -> None:
        """Create a ReplicationClient.

        Note that replicated_type must match T, one is used by the
        type checker, the other is available at runtime to help us
        deserialize the correct type.

        Args:
            endpoint: URL of the endpoint to connect to.
            validator: Validator to use to validate incoming updates.
        """
        self._endpoint = endpoint
        self._validator = validator

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
        self._validator.validate(self.UpdateType.__name__, update_json)
        logger.info(f'Validated against {self.UpdateType.__name__}')
        return deserialize(self.UpdateType, update_json)
