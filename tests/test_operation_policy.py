import re
import unittest
from swarm_sdk import SwarmClient
from swarm_sdk._operation_policies import CLIENT_OPERATION_POLICIES
from sdk_test_support import FakeTransport, json_response


class OperationPolicyTests(unittest.TestCase):
    def test_every_registered_mutation_keeps_its_declared_key_strategy(self):
        transport = FakeTransport()
        transport.responses = [json_response({}) for _ in range(len(CLIENT_OPERATION_POLICIES) * 2)]
        client = SwarmClient(base_url='https://local.invalid/prefix/', transport=transport)
        for method, template, required, strategy in CLIENT_OPERATION_POLICIES:
            if method == 'GET':
                continue
            path = re.sub(r'\{[^}]+\}', 'CaseSensitive_ID', template)
            for _ in range(2):
                client.request(method, path, body={'evidence': ' exact '})
            one, two = transport.requests[-2:]
            if not required:
                self.assertNotIn('Idempotency-Key', one['headers'])
            elif strategy == 'fresh-attempt':
                self.assertNotEqual(one['headers']['Idempotency-Key'], two['headers']['Idempotency-Key'])
            else:
                self.assertEqual(one['headers']['Idempotency-Key'], two['headers']['Idempotency-Key'])
            self.assertEqual(one['url'], two['url'])

    def test_unknown_operations_require_explicit_policy_or_caller_retry_key(self):
        transport = FakeTransport()
        transport.responses = [json_response({}) for _ in range(4)]
        client = SwarmClient(base_url='https://local.invalid', transport=transport)
        for path in ['/v2/runs/id/acquire', '/v1/runs/id/acquire/extra', '/v1/runs/id/not-a-command']:
            with self.assertRaisesRegex(ValueError, 'explicit idempotency'):
                client.post(path, {})
        self.assertEqual(transport.requests, [])
        for _ in range(2):
            client.post('/v2/command', {}, idempotency_policy='fresh-attempt')
        self.assertNotEqual(transport.requests[0]['headers']['Idempotency-Key'], transport.requests[1]['headers']['Idempotency-Key'])
        for _ in range(2):
            client.post('/v2/command', {}, idempotency_key='old-exact-retry')
        self.assertEqual(transport.requests[-1]['headers']['Idempotency-Key'], 'old-exact-retry')
        with self.assertRaisesRegex(ValueError, 'conflicts'):
            client.post('/v1/runs/id/acquire', {}, idempotency_policy='durable-content')
        with self.assertRaisesRegex(ValueError, 'Invalid'):
            client.post('/v2/command', {}, idempotency_policy='unknown')
        with self.assertRaisesRegex(ValueError, 'conflicts'):
            client.post('/v1/runs/id/acquire', {}, idempotency_key='caller-retry', idempotency_policy='durable-content')

    def test_query_is_part_of_durable_wire_identity_and_action_suffix_remains_literal(self):
        transport = FakeTransport()
        transport.responses = [json_response({}) for _ in range(2)]
        client = SwarmClient(base_url='https://local.invalid', transport=transport)
        client.post('/v1/artifacts', {}, query={'synthetic': 'one'})
        client.post('/v1/artifacts', {}, query={'synthetic': 'two'})
        self.assertNotEqual(transport.requests[0]['headers']['Idempotency-Key'], transport.requests[1]['headers']['Idempotency-Key'])
        with self.assertRaisesRegex(ValueError, 'explicit'):
            client.post('/v1/foundation/spaces/id/runs:invented', {})
