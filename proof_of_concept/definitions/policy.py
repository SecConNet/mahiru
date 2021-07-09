"""Classes for describing and managing policies."""
from proof_of_concept.definitions.signable import Signable
from proof_of_concept.util import ComparesByValue


class Rule(ComparesByValue, Signable):
    """Abstract base class for policy rules."""
    def signing_namespace(self) -> str:
        """Return the namespace whose owner must sign this rule."""
        raise NotImplemented
