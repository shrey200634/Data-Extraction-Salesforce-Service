

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend

from app.config import Settings


def _generate_pkcs8_pem() -> str:
    """Generate a valid 2048-bit RSA private key in PKCS#8 PEM format."""
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend(),
    )
    pem_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return pem_bytes.decode("utf-8")


def _generate_traditional_rsa_pem() -> str:
    """Generate a valid 2048-bit RSA private key in traditional RSA PEM format."""
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend(),
    )
    pem_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return pem_bytes.decode("utf-8")


def _current_fix_private_key(v: str) -> str:
   
    return v.replace("\\n", "\n")


class TestBugConditionPrivateKeyDeserialization:
    

    def test_case_1_literal_newline_escapes_single_quoted_env(self):
        
        valid_pem = _generate_pkcs8_pem()
        # Simulate single-quoted .env: replace real newlines with literal \n
        # (two characters: backslash + n)
        escaped_pem = valid_pem.replace("\n", "\\n")

        # Pass through current validator
        result = _current_fix_private_key(escaped_pem)

        # Assert it loads successfully
        key = serialization.load_pem_private_key(
            result.encode("utf-8"),
            password=None,
            backend=default_backend(),
        )
        assert key is not None
        assert key.key_size == 2048

    def test_case_2_residual_double_quotes(self):
       
        valid_pem = _generate_pkcs8_pem()
        # Simulate residual double quotes wrapping the escaped PEM
        escaped_pem = valid_pem.replace("\n", "\\n")
        quoted_pem = '"' + escaped_pem + '"'

        # Pass through current validator
        result = _current_fix_private_key(quoted_pem)

        # Assert it loads successfully
        key = serialization.load_pem_private_key(
            result.encode("utf-8"),
            password=None,
            backend=default_backend(),
        )
        assert key is not None
        assert key.key_size == 2048

    def test_case_3_missing_terminal_newline(self):
       
        valid_pem = _generate_pkcs8_pem()
        # Remove terminal newline
        pem_no_trailing = valid_pem.rstrip("\n")
        # Simulate dotenv delivery: escape newlines
        escaped_pem = pem_no_trailing.replace("\n", "\\n")

        # Pass through current validator
        result = _current_fix_private_key(escaped_pem)

        # Assert it loads successfully
        key = serialization.load_pem_private_key(
            result.encode("utf-8"),
            password=None,
            backend=default_backend(),
        )
        assert key is not None
        assert key.key_size == 2048

    def test_case_4_double_escaped_newlines(self):
       
        valid_pem = _generate_pkcs8_pem()
        # Double-escape: real newlines -> \\n (the string contains backslash+backslash+n)
        double_escaped_pem = valid_pem.replace("\n", "\\\\n")

        # Pass through current validator
        result = _current_fix_private_key(double_escaped_pem)

        # Assert it loads successfully
        key = serialization.load_pem_private_key(
            result.encode("utf-8"),
            password=None,
            backend=default_backend(),
        )
        assert key is not None
        assert key.key_size == 2048

    def test_case_5_traditional_rsa_key_with_literal_escapes(self):
       
        valid_pem = _generate_traditional_rsa_pem()
        # Simulate single-quoted .env: replace real newlines with literal \n
        escaped_pem = valid_pem.replace("\n", "\\n")

        # Pass through current validator
        result = _current_fix_private_key(escaped_pem)

        # Assert it loads successfully
        key = serialization.load_pem_private_key(
            result.encode("utf-8"),
            password=None,
            backend=default_backend(),
        )
        assert key is not None
        assert key.key_size == 2048
