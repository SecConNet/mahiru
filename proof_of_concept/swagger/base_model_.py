"""Base model for models used by swagger."""
import pprint
from typing import Dict, Any, Type

import six

from proof_of_concept.swagger import util


class Model(object):
    """Base model for models used by swagger."""
    # swaggerTypes: The key is attribute name and the
    # value is attribute type.
    swagger_types = {}  # type: Dict[str, Any]

    # attributeMap: The key is attribute name and the
    # value is json key in definition.
    attribute_map = {}  # type: Dict[str, str]

    @classmethod
    def from_dict(cls: Type, dikt: Dict) -> Any:
        """Returns the dict as a model."""
        return util.deserialize_model(dikt, cls)

    def to_dict(self) -> Dict[Any, Any]:
        """Returns the model properties as a dict."""
        result = {}

        for attr, _ in six.iteritems(self.swagger_types):
            value = getattr(self, attr)
            if isinstance(value, list):
                result[attr] = list(map(
                    lambda x: x.to_dict() if hasattr(x, "to_dict") else x,
                    value
                ))
            elif hasattr(value, "to_dict"):
                result[attr] = value.to_dict()
            elif isinstance(value, dict):
                result[attr] = dict(map(
                    lambda item: (item[0], item[1].to_dict())
                    if hasattr(item[1], "to_dict") else item,
                    value.items()
                ))
            else:
                result[attr] = value

        return result

    def to_str(self) -> str:
        """Returns the string representation of the model."""
        return pprint.pformat(self.to_dict())

    def __repr__(self) -> str:
        """For `print` and `pprint`."""
        return self.to_str()

    def __eq__(self, other: Any) -> bool:
        """Returns true if both objects are equal."""
        try:
            return self.__dict__ == other.__dict__
        except AttributeError:
            return False

    def __ne__(self, other: Any) -> bool:
        """Returns true if both objects are not equal."""
        return not self == other

    def __hash__(self) -> int:
        """Hash, just return python object id."""
        return id(self)
