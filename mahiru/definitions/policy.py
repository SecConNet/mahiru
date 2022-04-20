"""Classes for describing and managing policies."""
from mahiru.definitions.signable import Signable
from mahiru.util import ComparesByValue


class Rule(ComparesByValue, Signable):
    """Abstract base class for policy rules."""
    def signing_namespace(self) -> str:
        """Return the namespace whose owner must sign this rule."""
        raise NotImplementedError
