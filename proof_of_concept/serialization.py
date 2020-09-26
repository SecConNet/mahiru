"""(De)Serializes objects of various kinds to JSON."""
from datetime import datetime
from typing import (
        Any, Callable, cast, Dict, Generic, Mapping, Type, TypeVar, Union)

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import (
        Encoding, load_pem_public_key, PublicFormat)
from dateutil import parser as dateparser
import jsonschema
from jsonschema.validators import RefResolver
from openapi_schema_validator import OAS30Validator

from proof_of_concept.definitions import (
        PartyDescription, RegisteredObject, RegistryUpdate, ReplicaUpdate,
        SiteDescription)


JSON = Any


ValidationError = jsonschema.ValidationError


Serializable = Union[RegisteredObject, ReplicaUpdate]


_SerializableT = TypeVar('_SerializableT', bound=Serializable)


def serialize_party_description(party_desc: PartyDescription) -> JSON:
    """Serializes a PartyDescription object to JSON."""
    public_key = party_desc.public_key.public_bytes(
            encoding=Encoding.PEM,
            format=PublicFormat.SubjectPublicKeyInfo
            ).decode('ascii')

    return {
            'name': party_desc.name,
            'public_key': public_key}


def serialize_site_description(site_desc: SiteDescription) -> JSON:
    """Serializes a SiteDescription object to JSON."""
    result = dict()     # type: JSON
    result['name'] = site_desc.name
    result['owner_name'] = site_desc.owner_name
    result['admin_name'] = site_desc.admin_name
    # TODO: enable these when those things have endpoints
    # if site_desc.runner is not None:
    #     result['runner_endpoint'] = site_desc.runner_endpoint
    # if site_desc.store is not None:
    #     result['store_endpoint'] = site_desc.store_endpoint
    if site_desc.namespace is not None:
        result['namespace'] = site_desc.namespace
    # if site_desc.policy_server_endpoint is not None:
    #     result['policy_server_endpoint'] = \
    #         site_desc.policy_server_endpoint
    return result


def serialize_replica_update(update: ReplicaUpdate[_SerializableT]) -> JSON:
    """Serialize a replica update to JSON."""
    result = dict()     # type: JSON
    result['from_version'] = update.from_version
    result['to_version'] = update.to_version
    dt = datetime.fromtimestamp(update.valid_until)
    result['valid_until'] = dt.isoformat()
    result['created'] = [serialize(o) for o in update.created]
    result['deleted'] = [serialize(o) for o in update.deleted]
    return result


_serializers = dict()   # type: Dict[Type, Callable[[Any], JSON]]
_serializers = {
        PartyDescription: serialize_party_description,
        SiteDescription: serialize_site_description,
        ReplicaUpdate: serialize_replica_update}


def serialize(obj: Serializable) -> JSON:
    """Serializes objects to JSON dicts."""
    return _serializers[type(obj)](obj)


class PartyDescriptionDeserializer:
    """Deserializes a PartyDescription object from JSON."""
    def __init__(self, schema: JSON) -> None:
        """Create a PartyDescriptionDeserializer.

        Args:
            schema: An OpenAPI schema to check against.
        """
        ref_resolver = RefResolver.from_schema(schema)
        self._validator = OAS30Validator(
                schema['components']['schemas']['Party'],
                resolver=ref_resolver)

    def __call__(self, user_input: JSON) -> PartyDescription:
        """Deserializes a PartyDescription.

        This validates first, then returns a PartyDescription object.

        Args:
            user_input: Untrusted user input, JSON objects.

        Returns:
            The deserialized PartyDescription object.

        Raises:
            ValidationError: If the input was invalid.
        """
        self._validator.validate(user_input)
        name = user_input['name']
        public_key = load_pem_public_key(
                user_input['public_key'].encode('ascii'), default_backend())
        return PartyDescription(name, public_key)


class SiteDescriptionDeserializer:
    """Deserializes a SiteDescription object from JSON."""
    def __init__(self, schema: JSON) -> None:
        """Create a SiteDescriptionDeserializer.

        Args:
            schema: An OpenAPI schema to check against.
        """
        ref_resolver = RefResolver.from_schema(schema)
        self._validator = OAS30Validator(
                schema['components']['schemas']['Site'],
                resolver=ref_resolver)

    def __call__(self, user_input: JSON) -> SiteDescription:
        """Deserializes a SiteDescription.

        This validates first, then returns a PartyDescription object.

        Args:
            user_input: Untrusted user input, JSON objects.

        Returns:
            The deserialized PartyDescription object.

        Raises:
            ValidationError: If the input was invalid.
        """
        self._validator.validate(user_input)
        try:
            return SiteDescription(
                    user_input['name'],
                    user_input['owner_name'],
                    user_input['admin_name'],
                    None,
                    # user_input.get('runner_endpoint'),
                    None,
                    # user_input.get('store_endpoint'),
                    None,
                    # user_input.get('namespace'),
                    None
                    # user_input.get('policy_server_endpoint')
                    )
        except RuntimeError:
            raise ValidationError('Invalid input')


class RegisteredObjectDeserializer:
    """Deserializer a RegisteredObject from JSON."""
    def __init__(self, schema: JSON) -> None:
        """Create a RegisteredObjectDeserializer.

        Args:
            schema: An OpenAPI schema to check against.
        """
        self._site_deserializer = SiteDescriptionDeserializer(schema)
        self._party_deserializer = PartyDescriptionDeserializer(schema)

    def __call__(self, user_input: JSON) -> RegisteredObject:
        """Deserialize a RegisteredObject.

        This validates first, then returns either a PartyDescription
        or a SiteDescription object.

        Args:
            user_input: The untrusted user input.

        Returns:
            The deserialized RegisteredObject object.

        Raises:
            ValidationError: If the input was invalid.
        """
        try:
            result = self._site_deserializer(
                    user_input)    # type: RegisteredObject
        except ValidationError:
            result = self._party_deserializer(user_input)
        return result


class Deserializer:
    """A utility class for deserialising objects."""
    def __init__(self, api_def: JSON) -> None:
        """Create a Deserializer."""
        self._deserializers = dict()  # type: Dict[Type, Callable[[JSON], Any]]
        self._deserializers = {
                PartyDescription: PartyDescriptionDeserializer(api_def),
                SiteDescription: SiteDescriptionDeserializer(api_def),
                RegisteredObject: RegisteredObjectDeserializer(api_def),
                }

    def __call__(
            self, expected_type: Type[_SerializableT], user_input: JSON
            ) -> _SerializableT:
        """Deserialize user input into an expected type.

        This validates first, then returns an object of the expected
        type.

        Args:
            user_input: Untrusted user input, JSON objects.

        Returns:
            The deserialized object.

        Raises:
            ValidationError: If the input was invalid.
        """
        return cast(
                _SerializableT,
                self._deserializers[expected_type](user_input))


T = TypeVar('T')


class ReplicaUpdateDeserializer(Generic[T]):
    """Deserializes a ReplicaUpdate from JSON.

    Note that this is special, being the only class that you don't
    deserialize through the generic Deserializer, but directly via
    a specific deserializer class. That's because this is a generic
    class, which uses Deserializer itself because it doesn't know
    what T is. If it was also in Deserializer itself, we'd have a
    loop and get in trouble.

    """
    def __init__(
            self, schema: JSON, name: str,
            replicated_type: Type) -> None:
        """Create a ReplicaUpdateDeserializer.

        Note that replicated_type must be the same as T; one is for
        the type checker, the other is available at runtime so that
        we can deserialize the correct type.

        Args:
            schema: An OpenAPI schema to check against.
            name: Name of the OpenAPI type to check against.
            replicated_type: The type of the objects being
                replicated.
        """
        ref_resolver = RefResolver.from_schema(schema)
        self._validator = OAS30Validator(
                schema['components']['schemas'][name], resolver=ref_resolver)
        self._deserializer = Deserializer(schema)
        self._replicated_type = replicated_type

    def __call__(self, user_input: JSON) -> ReplicaUpdate[T]:
        """Deserialize a ReplicaUpdate.

        This validates first, then returns a ReplicaUpdate[T] object.

        Args:
            user_input: Untrusted user input, JSON objects.

        Returns:
            The deserialized ReplicaUpdate object.

        Raises:
            ValidationError: If the input was invalid.
        """
        self._validator.validate(user_input)
        return ReplicaUpdate[T](
                user_input['from_version'],
                user_input['to_version'],
                dateparser.isoparse(user_input['valid_until']).timestamp(),
                {self._deserializer(self._replicated_type, o)
                    for o in user_input['created']},
                {self._deserializer(self._replicated_type, o)
                    for o in user_input['deleted']})
