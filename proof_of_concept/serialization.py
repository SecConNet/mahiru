"""(De)Serializes objects of various kinds to JSON."""
from datetime import datetime
from typing import (
        Any, Callable, cast, Dict, Generic, Mapping, Type, TypeVar, Union)

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import (
        Encoding, load_pem_public_key, PublicFormat)
from dateutil import parser as dateparser

from proof_of_concept.definitions import (
        JSON, PartyDescription, RegisteredObject, RegistryUpdate,
        ReplicaUpdate, SiteDescription)


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


def deserialize_party_description(user_input: JSON) -> PartyDescription:
    """Deserializes a PartyDescription.

    Be sure to validate first if the input is untrusted.

    Args:
        user_input: Untrusted user input, JSON objects.

    Returns:
        The deserialized PartyDescription object.
    """
    name = user_input['name']
    public_key = load_pem_public_key(
            user_input['public_key'].encode('ascii'), default_backend())
    return PartyDescription(name, public_key)


def deserialize_site_description(user_input: JSON) -> SiteDescription:
    """Deserializes a SiteDescription.

    Be sure to validate first if the input is untrusted.

    Args:
        user_input: Untrusted user input, JSON objects.

    Returns:
        The deserialized SiteDescription object.
    """
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


def deserialize_registered_object(user_input: JSON) -> RegisteredObject:
    """Deserialize a RegisteredObject.

    Be sure to validate first if the input is untrusted.

    Args:
        user_input: The untrusted user input.

    Returns:
        The deserialized RegisteredObject object.
    """
    if 'public_key' in user_input:
        return deserialize_party_description(user_input)
    return deserialize_site_description(user_input)


_deserialize = {
        'Site': deserialize_site_description,
        'Party': deserialize_party_description,
        'RegisteredObject': deserialize_registered_object
        }    # type: Dict[str, Callable[[JSON], Any]]


T = TypeVar('T')


def deserialize_replica_update(
        content_type_tag: str,
        user_input: JSON
        ) -> ReplicaUpdate[T]:
    """Deserialize a ReplicaUpdate.

    Be sure to validate first if the input is untrusted.

    Args:
        content_type_tag: Name of the type to be deserialized.
        user_input: Untrusted user input, JSON objects.

    Returns:
        The deserialized ReplicaUpdate object.
    """
    return ReplicaUpdate[T](
            user_input['from_version'],
            user_input['to_version'],
            dateparser.isoparse(user_input['valid_until']).timestamp(),
            {_deserialize[content_type_tag](o)
                for o in user_input['created']},
            {_deserialize[content_type_tag](o)
                for o in user_input['deleted']})
