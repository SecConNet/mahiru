"""Small generic utilities."""
from typing import Any


class ComparesByValue:
    """Base class for sort-of value types.

    Python doesn't have copy constructors, so we cannot make true
    value types, but we can make classes that compare by value. This
    base class implements that.
    """
    def __eq__(self, other: Any) -> bool:
        """Return whether this object equals the other one."""
        return type(self) == type(other) and self.__dict__ == other.__dict__

    def __hash__(self) -> int:
        """Return a hash value for this object."""
        sorted_items = sorted(self.__dict__.items())
        return hash((self.__class__.__name__, tuple(sorted_items)))
