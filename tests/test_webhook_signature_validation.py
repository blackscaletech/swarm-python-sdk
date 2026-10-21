import hashlib
import hmac
import unittest

from swarm_sdk import compute_webhook_signature, verify_webhook_signature


SECRET = "synthetic-signing-secret"
BODY = '{"message":"\u00e9"}'
SIGNATURE = "sha256=" + hmac.new(SECRET.encode(), BODY.encode(), hashlib.sha256).hexdigest()


class WebhookSignatureValidationTests(unittest.TestCase):
    def test_malformed_signatures_return_false_without_throwing(self):
        malformed = (None, 42, {}, "", " ", "sha256=", "sha256=" + "0" * 63,
                     "sha256=" + "0" * 65, "sha256=" + "g" * 64, "sha256=" + "A" * 64,
                     "SHA256=" + "0" * 64, "sha256=" + "\u00e9" * 64, "sha256=" + "\u00e9" * 32,
                     "sha256=" + "\U0001f4a5" * 32, "sha256=" + "0" * 63 + "\0")
        for signature in malformed:
            with self.subTest(signature=signature):
                self.assertFalse(verify_webhook_signature(secret=SECRET, body=BODY, signature=signature))

    def test_valid_signatures_whitespace_and_bytes_are_preserved(self):
        for body in (BODY, BODY.encode(), bytearray(BODY.encode()), memoryview(BODY.encode())):
            with self.subTest(type=type(body)):
                self.assertEqual(compute_webhook_signature(SECRET, body), SIGNATURE)
                self.assertTrue(verify_webhook_signature(secret=SECRET, body=body, signature=SIGNATURE))
                self.assertTrue(verify_webhook_signature(secret=SECRET, body=body, signature=" \t" + SIGNATURE + "\r\n"))

    def test_wrong_digests_changed_bodies_and_unsupported_versions_remain_invalid(self):
        self.assertFalse(verify_webhook_signature(secret=SECRET, body=BODY, signature="sha256=" + "0" * 64))
        self.assertFalse(verify_webhook_signature(secret=SECRET, body=BODY + " ", signature=SIGNATURE))
        for version in ("v2", "", None):
            self.assertFalse(verify_webhook_signature(secret=SECRET, body=BODY, signature=SIGNATURE, signature_version=version))

    def test_valid_format_still_surfaces_signing_secret_configuration_errors(self):
        with self.assertRaisesRegex(ValueError, "non-empty signing secret"):
            verify_webhook_signature(secret="", body=BODY, signature=SIGNATURE)
