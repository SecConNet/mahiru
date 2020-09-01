import datetime
from typing import Union, List, Dict, Any, ClassVar, GenericMeta

import six


def _deserialize(data: Union[Dict, List, str], klass: ClassVar):
    """Deserializes dict, list, str into an object.

    Args:
        data: dict, list or str.
        klass: class literal, or string of class name.

    Returns:
        The object
    """
    if data is None:
        return None

    if klass in six.integer_types or klass in (float, str, bool):
        return _deserialize_primitive(data, klass)
    elif klass == object:
        return _deserialize_object(data)
    elif klass == datetime.date:
        return deserialize_date(data)
    elif klass == datetime.datetime:
        return deserialize_datetime(data)
    elif type(klass) == GenericMeta:
        if klass.__extra__ == list:
            return _deserialize_list(data, klass.__args__[0])
        if klass.__extra__ == dict:
            return _deserialize_dict(data, klass.__args__[1])
    else:
        return deserialize_model(data, klass)


def _deserialize_primitive(data: Any, klass: ClassVar
                           ) -> Union[int, float, str, bool]:
    """Deserializes to primitive type.

    Args:
        data: data to deserialize.
        klass: class literal.

    Returns:
        deserialized primitive
    """
    try:
        value = klass(data)
    except UnicodeEncodeError:
        value = six.u(data)
    except TypeError:
        value = data
    return value


def _deserialize_object(value: Any) -> Any:
    """Return a original value."""
    return value


def deserialize_date(string: str):
    """Deserializes string to date."""
    try:
        from dateutil.parser import parse
        return parse(string).date()
    except ImportError:
        return string


def deserialize_datetime(string: str):
    """Deserializes string to datetime."""
    try:
        from dateutil.parser import parse
        return parse(string)
    except ImportError:
        return string


def deserialize_model(data: Union[Dict, List], klass: ClassVar):
    """Deserializes list or dict to model."""
    instance = klass()

    if not instance.swagger_types:
        return data

    for attr, attr_type in six.iteritems(instance.swagger_types):
        if data is not None \
                and isinstance(data, (list, dict)) \
                and instance.attribute_map[attr] in data:
            value = data[instance.attribute_map[attr]]
            setattr(instance, attr, _deserialize(value, attr_type))

    return instance


def _deserialize_list(data: List, boxed_type: ClassVar) -> List:
    """Deserializes a list and its elements."""
    return [_deserialize(sub_data, boxed_type)
            for sub_data in data]


def _deserialize_dict(data: Dict, boxed_type: ClassVar) -> Dict:
    """Deserializes a dict and its elements."""
    return {k: _deserialize(v, boxed_type)
            for k, v in six.iteritems(data)}
