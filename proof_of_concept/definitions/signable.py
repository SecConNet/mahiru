"""Support for cryptographically signed objects."""
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import (
        RSAPrivateKey, RSAPublicKey)


class Signable:
    """An abstract base class for signable classes."""
    signature = None      # type: bytes

    def sign(self, key: RSAPrivateKey) -> None:
        """Sign the object.

        Args:
            key: The private key to use.
        """
        message = self.signing_representation()
        self.signature = key.sign(
                message,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH),
                hashes.SHA256())

    def has_valid_signature(self, key: RSAPublicKey) -> bool:
        """Verify the signature on the object.

        Args:
            key: The public key to use.

        Return:
            True iff there is a valid signature.
        """
        if self.signature is None:
            return False

        message = self.signing_representation()
        try:
            key.verify(
                    self.signature, message,
                    padding.PSS(
                        mgf=padding.MGF1(hashes.SHA256()),
                        salt_length=padding.PSS.MAX_LENGTH),
                    hashes.SHA256())
            return True
        except InvalidSignature:
            return False

    def signing_representation(self) -> bytes:
        """Return a string of bytes representing the object.

        This should contain all the information that is to be included
        in the signature, and must be overridden and implemented by the
        derived class.
        """
        raise NotImplementedError()
