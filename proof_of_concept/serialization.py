"""(De)Serializes objects of various kinds to JSON."""
from typing import Any, Callable, cast, Dict, Mapping, Type, TypeVar, Union

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import (
        Encoding, load_pem_public_key, PublicFormat)
import jsonschema
from openapi_schema_validator import OAS30Validator
from pathlib import Path
import ruamel.yaml as yaml

from proof_of_concept.definitions import PartyDescription, SiteDescription


ValidationError = jsonschema.ValidationError


Serializable = Union[PartyDescription, SiteDescription]


_SerializableT = TypeVar('_SerializableT', bound=Serializable)


def serialize_party_description(party_desc: PartyDescription) -> Any:
    """Serializes a PartyDescription object to JSON."""
    public_key = party_desc.public_key.public_bytes(
            encoding=Encoding.PEM,
            format=PublicFormat.SubjectPublicKeyInfo
            ).decode('ascii')

    return {
            'name': party_desc.name,
            'public_key': public_key}


def serialize_site_description(site_desc: SiteDescription) -> Any:
    """Serializes a SiteDescription object to JSON."""
    result = dict()     # type: Dict[str, Any]
    result['name'] = site_desc.name
    result['owner_name'] = site_desc.owner_name
    result['admin_name'] = site_desc.admin_name
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


_serializers = dict()   # type: Dict[Type, Callable[[Any], Any]]
_serializers = {
        PartyDescription: serialize_party_description,
        SiteDescription: serialize_site_description}


def serialize(obj: Serializable) -> Any:
    """Serializes objects to JSON dicts."""
    return _serializers[type(obj)](obj)


class PartyDescriptionDeserializer:
    """Deserializes a PartyDescription object from JSON."""
    def __init__(self, schema: Dict[str, Any]) -> None:
        """Create a PartyDescriptionDeserializer.

        Args:
            schema: An OpenAPI schema to check against.
        """
        self._validator = OAS30Validator(schema['Party'])

    def __call__(self, user_input: Any) -> PartyDescription:
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
    def __init__(self, schema: Dict[str, Any]) -> None:
        """Create a SiteDescriptionDeserializer.

        Args:
            schema: An OpenAPI schema to check against.
        """
        self._validator = OAS30Validator(schema['Site'])

    def __call__(self, user_input: Any) -> SiteDescription:
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


class Deserializer:
    """A utility class for deserialising objects."""
    def __init__(self) -> None:
        """Create a Deserializer."""
        registry_api_file = Path(__file__).parent / 'registry_api.yaml'
        with open(registry_api_file, 'r') as f:
            registry_api_def = yaml.safe_load(f.read())
        schemas = registry_api_def['components']['schemas']

        # Spent a few hours trying to get this to type-check, but mypy
        # just doesn't want to do it...
        self._deserializers = dict()  # type: Any
        self._deserializers = {
                PartyDescription: PartyDescriptionDeserializer(schemas),
                SiteDescription: SiteDescriptionDeserializer(schemas)
                }

    def __call__(
            self, expected_type: Type[_SerializableT], user_input: Any
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
