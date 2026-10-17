import unittest
from swarm_sdk._client_security import _sanitize_sensitive_text


class ConnectionTokenRedaction(unittest.TestCase):
    def test_versioned_and_legacy_proofs_keep_full_punctuation_boundaries(self):
        key, family = "a" * 16, "b" * 32
        for end in ["x", "-", "_"]:
            proof = "q" * 42 + end
            tokens = [f"swa_v2.{key}.{proof}", f"swc_v2.{key}.{proof}", f"swac_v2.{key}.{proof}",
                      f"swr_ctf_{family}.17.v2.{key}.{proof}", f"swr_ctf_{family}.{proof}"]
            tokens.extend(prefix + proof for prefix in ["swa_", "swc_", "swu_", "swur_"])
            for token in tokens:
                self.assertEqual(_sanitize_sensitive_text(f"Rejected proof {token}."), "Rejected proof [REDACTED_SECRET].")
        public_id = f"swc_client_reg_{family}"
        self.assertEqual(_sanitize_sensitive_text(public_id), public_id)
