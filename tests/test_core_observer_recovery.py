import json
import threading
import unittest

from swarm_sdk import SwarmClient, execute_connected_core_acquisition, SwarmTimeoutError, SwarmCoreExecutionUncertainError
from swarm_sdk.client import HTTPResponse
from swarm_sdk.connected_core_runtime import _LeaseRenewal


class CoreObserverRecoveryTests(unittest.TestCase):
    def test_lost_start_execute_and_runner_required_do_not_mutate_or_reexecute(self):
        for target in ['start', 'execute', 'runner_required']:
            with self.subTest(target=target):
                actions = []

                class Transport:
                    def request(self, method, url, headers, body, timeout):
                        action = url.rsplit('/', 1)[-1]
                        actions.append(action)
                        if action == target:
                            raise ConnectionError('synthetic lost response')
                        data = {'requires_runner': True, 'run': {'id': 'run_synthetic', 'status': 'running'}} if action == 'execute' else {'id': 'lease_synthetic', 'status': 'active'}
                        return HTTPResponse(200, {'content-type': 'application/json'}, json.dumps(data).encode())

                with self.assertRaises(SwarmCoreExecutionUncertainError) as caught:
                    execute_connected_core_acquisition(SwarmClient(base_url='https://synthetic.invalid', transport=Transport()),
                                                       {'run': {'id': 'run_synthetic'}, 'run_lease': {'id': 'lease_synthetic'}})
                self.assertEqual(caught.exception.stage, target)
                self.assertEqual(caught.exception.run_id, 'run_synthetic')
                self.assertFalse(caught.exception.retryable)
                self.assertNotIn('complete', actions)
                self.assertNotIn('release', actions)
                self.assertLessEqual(actions.count('execute'), 1)

    def test_observer_failure_after_known_success_does_not_complete_failed_or_release(self):
        actions = []

        class Transport:
            def request(self, method, url, headers, body, timeout):
                action = url.rsplit('/', 1)[-1]
                actions.append(action)
                data = {'run': {'id': 'run_synthetic', 'status': 'succeeded'}} if action == 'execute' else {'id': 'lease_synthetic', 'status': 'active'}
                return HTTPResponse(200, {'content-type': 'application/json'}, json.dumps(data).encode())

        failure = RuntimeError('synthetic observer failure')

        def observer(_):
            raise failure

        with self.assertRaises(RuntimeError) as caught:
            execute_connected_core_acquisition(SwarmClient(base_url='https://synthetic.invalid', transport=Transport()),
                                               {'run': {'id': 'run_synthetic'}, 'run_lease': {'id': 'lease_synthetic'}}, on_execution=observer)
        self.assertIs(caught.exception, failure)
        self.assertEqual(actions, ['start', 'renew', 'execute'])

    def test_cleanup_is_bounded_when_a_custom_thread_ignores_cancellation(self):
        release = threading.Event()
        renewal = _LeaseRenewal(SwarmClient(base_url='https://synthetic.invalid'), 'run', 'lease', 60, None)
        renewal._thread = threading.Thread(target=release.wait, daemon=True)
        renewal._thread.start()
        try:
            with self.assertRaises(SwarmTimeoutError) as caught:
                renewal.close()
            self.assertEqual(caught.exception.kind, 'lease_cleanup')
            self.assertFalse(caught.exception.retryable)
            self.assertTrue(renewal._stop.is_set())
        finally:
            release.set()
            renewal._thread.join(timeout=2)
        self.assertFalse(renewal._thread.is_alive())
