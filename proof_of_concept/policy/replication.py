"""Support for replication of policies."""
import logging

from proof_of_concept.definitions.policy import Rule
from proof_of_concept.policy.definitions import PolicyUpdate
from proof_of_concept.policy.rules import (
        InAssetCollection, InPartyCollection, MayAccess, ResultOfIn,
        ResultOfDataIn, ResultOfComputeIn)
from proof_of_concept.replication import CanonicalStore, ObjectValidator

from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey


logger = logging.getLogger(__name__)


class RuleValidator(ObjectValidator[Rule]):
    """Validates incoming policy rules by checking signatures."""
    def __init__(self, namespace: str, key: RSAPublicKey) -> None:
        """Create a RuleValidator.

        Checks that rules apply to the given namespace, and that they
        have been signed by the owner of that namespace.

        Args:
            namespace: The namespace to expect rules for.
            key: The key to validate the rules with.
        """
        self._namespace = namespace
        self._key = key

    def is_valid(self, rule: Rule) -> bool:
        """Return True iff the rule is properly signed."""
        if rule.signing_namespace() != self._namespace:
            return False
        return rule.has_valid_signature(self._key)


class PolicyStore(CanonicalStore[Rule]):
    """A canonical store for policy rules."""
    UpdateType = PolicyUpdate
