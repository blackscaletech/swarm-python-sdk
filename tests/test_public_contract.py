from __future__ import annotations

import hashlib
from importlib.resources import files
import json
import unittest

from swarm_sdk._operation_policies import CLIENT_OPERATION_POLICIES


class PublicContractTests(unittest.TestCase):
    def test_contract_is_the_reviewed_public_snapshot(self):
        raw = files("swarm_sdk").joinpath("client-contract.json").read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(),
                         "6ff272e88ef4af66d833d3912bbdf12ac49b1f13f5bc700bd98cb7d5e89801c5")
        contract = json.loads(raw)
        self.assertEqual(contract["schemaVersion"], "swarm.sdk-contract.v1")
        self.assertEqual(contract["apiVersion"], "v1")
        self.assertEqual(set(contract["fields"]), {
            "runRequirements", "scoringContract", "scoringDimension", "runContextRef", "launchRun",
        })
        for name, fields in contract["fields"].items():
            with self.subTest(name=name):
                self.assertEqual(fields, sorted(set(fields)))
                self.assertTrue(all(isinstance(field, str) and field for field in fields))

    def test_entire_operation_policy_matches_without_regeneration(self):
        contract = json.loads(files("swarm_sdk").joinpath("client-contract.json").read_bytes())
        self.assertEqual(tuple(tuple(row) for row in contract["operationPolicies"]), CLIENT_OPERATION_POLICIES)
        self.assertEqual(len(CLIENT_OPERATION_POLICIES), 302)

    def test_type_marker_is_available_to_installed_consumers(self):
        self.assertTrue(files("swarm_sdk").joinpath("py.typed").is_file())


if __name__ == "__main__":
    unittest.main()
