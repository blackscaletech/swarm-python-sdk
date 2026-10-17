from __future__ import annotations

import hashlib
import threading
import time
import traceback
import unittest
from unittest.mock import patch

from swarm_sdk import (SwarmArtifactIntegrityError, SwarmCancelledError, SwarmClient,
                       SwarmResponseLimitError, SwarmTimeoutError)
from sdk_test_support import FakeTransport, json_response


def content(body="body", artifact_id="artifact"):
    encoded = body.encode("utf-8")
    return {"artifact_id": artifact_id, "body": body, "media_type": "text/plain",
            "size_bytes": len(encoded), "sha256": hashlib.sha256(encoded).hexdigest()}


def fixture(payload, **options):
    transport = FakeTransport()
    transport.responses = [json_response(payload)]
    return SwarmClient(base_url="https://local.invalid", transport=transport, **options), transport


class VerifiedArtifactTests(unittest.TestCase):
    def test_utf8_content_empty_and_multimegabyte_are_verified_without_changing_response(self):
        for body in ("", "\u00e9\U0001f680", "x" * (2 * 1024 * 1024)):
            payload = content(body)
            client, _ = fixture(payload)
            result = client.get_verified_artifact_content("artifact", expected_sha256=payload["sha256"].upper())
            self.assertEqual(result, payload)

    def test_artifact_ids_are_escaped_as_one_path_segment_for_both_helpers(self):
        identifier = "artifact/other?query=#fragment"
        client, transport = fixture(content(artifact_id=identifier))
        client.get_verified_artifact_content(identifier)
        self.assertEqual(transport.requests[0]["url"],
                         "https://local.invalid/v1/artifacts/artifact%2Fother%3Fquery%3D%23fragment/content")
        transport.responses = [json_response({})]
        client.get_artifact_content(identifier)
        self.assertEqual(transport.requests[0]["url"], transport.requests[1]["url"])

    def test_invalid_inputs_do_not_open_transport(self):
        client, transport = fixture({})
        for identifier in ("", ".", "..", None, 1):
            with self.assertRaises(ValueError):
                client.get_verified_artifact_content(identifier)
        for digest in ("", "g" * 64, "0" * 63, 1, True):
            with self.assertRaises(ValueError):
                client.get_verified_artifact_content("artifact", expected_sha256=digest)
        for limit in (0, -1, True, 1.5):
            with self.assertRaises(ValueError):
                client.get_verified_artifact_content("artifact", max_artifact_bytes=limit)
        self.assertEqual(transport.requests, [])

    def test_id_size_hash_expected_revision_and_malformed_metadata_fail_without_content(self):
        baseline = content("private-secret")
        invalid = [None, {**baseline, "artifact_id": "other"}, {**baseline, "body": 1},
                   {**baseline, "size_bytes": True}, {**baseline, "size_bytes": -1},
                   {**baseline, "size_bytes": 2**53}, {**baseline, "size_bytes": 1},
                   {**baseline, "sha256": "g" * 64}, {**baseline, "sha256": "0" * 64},
                   {**baseline, "body": "\ud800"}, {**baseline, "body": "changed"}]
        for payload in invalid:
            client, _ = fixture(payload)
            with self.subTest(payload=payload), self.assertRaises(SwarmArtifactIntegrityError) as caught:
                client.get_verified_artifact_content("artifact")
            self.assertIsNone(caught.exception.__context__)
            self.assertNotIn("private-secret", "".join(traceback.format_exception(caught.exception)))
        client, _ = fixture(baseline)
        with self.assertRaises(SwarmArtifactIntegrityError):
            client.get_verified_artifact_content("artifact", expected_sha256="0" * 64)

    def test_artifact_byte_limit_counts_utf8_not_characters_even_if_metadata_lies(self):
        for size in (4, 1):
            client, _ = fixture({**content("\u00e9\u00e9"), "size_bytes": size})
            with self.assertRaises(SwarmResponseLimitError) as caught:
                client.get_verified_artifact_content("artifact", max_artifact_bytes=3)
            self.assertEqual(caught.exception.max_bytes, 3)

    def test_wire_limit_remains_independent_of_decoded_limit_and_route_override(self):
        payload = content("text")
        client, _ = fixture(payload, max_response_bytes=1, response_byte_limits=[
            {"method": "GET", "path": "/v1/artifacts/{id}/content", "max_response_bytes": 1000}])
        self.assertEqual(client.get_verified_artifact_content("artifact", max_artifact_bytes=4), payload)
        client, _ = fixture(payload)
        with self.assertRaises(SwarmResponseLimitError):
            client.get_verified_artifact_content("artifact", max_artifact_bytes=4, max_response_bytes=1)

    def test_precancellation_never_opens_transport(self):
        client, transport = fixture(content())
        stop = threading.Event()
        stop.set()
        with self.assertRaises(SwarmCancelledError):
            client.get_verified_artifact_content("artifact", stop_event=stop)
        self.assertEqual(transport.requests, [])

    def test_cancellation_during_hashing_prevents_verified_return(self):
        payload = content("x" * 50000)
        client, _ = fixture(payload)
        stop = threading.Event()
        original = hashlib.sha256

        class Digest:
            def __init__(self):
                self.digest = original()

            def update(self, chunk):
                self.digest.update(chunk)
                stop.set()

            def hexdigest(self):
                return self.digest.hexdigest()

        with patch("swarm_sdk._client_endpoints_operations.hashlib.sha256", side_effect=Digest):
            with self.assertRaises(SwarmCancelledError):
                client.get_verified_artifact_content("artifact", stop_event=stop)
        self.assertIsNone(client._operation.get())

    def test_fetch_and_hashing_share_one_total_operation_budget(self):
        payload = content("x" * 50000)
        client, _ = fixture(payload)
        original = hashlib.sha256

        class Digest:
            def __init__(self):
                self.digest = original()

            def update(self, chunk):
                time.sleep(0.03)
                self.digest.update(chunk)

            def hexdigest(self):
                return self.digest.hexdigest()

        with patch("swarm_sdk._client_endpoints_operations.hashlib.sha256", side_effect=Digest):
            with self.assertRaises(SwarmTimeoutError) as caught:
                client.get_verified_artifact_content("artifact", operation_timeout=0.01)
        self.assertEqual(caught.exception.kind, "operation")
        self.assertIsNone(client._operation.get())


if __name__ == "__main__":
    unittest.main()
