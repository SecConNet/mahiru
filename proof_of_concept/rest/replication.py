"""REST API handlers/clients for the replication system."""
from datetime import datetime, timedelta
import logging
import requests
from typing import Dict, Generic, Optional, Type, TypeVar

from falcon import Request, Response

from proof_of_concept.definitions.interfaces import IReplicationService
from proof_of_concept.definitions.registry import RegisteredObject
from proof_of_concept.definitions.policy import Rule
from proof_of_concept.policy.replication import PolicyUpdate
from proof_of_concept.registry.replication import RegistryUpdate
from proof_of_concept.replication import ReplicaUpdate
from proof_of_concept.rest.serialization import serialize, deserialize
from proof_of_concept.rest.validation import Validator


logger = logging.getLogger(__name__)


T = TypeVar('T')


class ReplicationHandler(Generic[T]):
    """A handler for a /updates REST API endpoint."""
    def __init__(self, service: IReplicationService[T]) -> None:
        """Create a Replication handler.

        Args:
            service: The service to get updates from.
        """
        self._service = service

    def on_get(self, request: Request, response: Response) -> None:
        """Handle a registry update request.

        Args:
            request: The submitted request.
            response: A response object to configure.
        """
        from_version = request.get_param_as_int(
                'from_version', required=True)

        updates = self._service.get_updates_since(from_version)
        response.media = serialize(updates)


class ReplicationRestClient(IReplicationService[T]):
    """Client for a ReplicationHandler REST endpoint."""
    UpdateType = ReplicaUpdate[T]   # type: Type[ReplicaUpdate[T]]

    def __init__(self, endpoint: str, validator: Validator) -> None:
        """Create a ReplicationRestClient.

        Note that UpdateType must be set to ReplicaUpdate[T] with the
        actual T when using this, one is used by the type checker, the
        other is available at runtime to help us deserialize the
        correct type.

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

        start_time = datetime.now()
        r = None
        while datetime.now() < start_time + timedelta(seconds=100):
            try:
                r = requests.get(self._endpoint, params=params)
                break
            except requests.ConnectionError:
                pass

        if r is None:
            raise RuntimeError('Could not connect to registry')

        update_json = r.json()
        logger.info(f'Replication update: {update_json}')
        self._validator.validate(self.UpdateType.__name__, update_json)
        logger.info(f'Validated against {self.UpdateType.__name__}')
        return deserialize(self.UpdateType, update_json)


class PolicyRestClient(ReplicationRestClient[Rule]):
    """A client for policy servers."""
    UpdateType = PolicyUpdate


class RegistryRestClient(ReplicationRestClient[RegisteredObject]):
    """A client for the registry."""
    UpdateType = RegistryUpdate
