import unittest

from swarm_sdk import SwarmClient
from sdk_test_support import FakeTransport, json_response


class HandoffProvenanceTests(unittest.TestCase):
    def test_answer_evidence_snapshots_preserved_without_legacy_inference(self):
        captured = {'space_id': 'sp_test', 'kind': 'direct_artifact', 'artifact_id': 'art_test', 'rank': 0,
                    'snapshot_version': 1, 'artifact_sha256': 'a' * 64, 'artifact_hash_status': 'recorded',
                    'artifact_created_at': '2026-10-14T12:00:00Z', 'logical_head_updated_at': '2026-10-14T13:00:00Z',
                    'projection_version': 3, 'projection_updated_at': '2026-10-14T11:00:00Z', 'created_at': '2026-10-14T14:00:00Z'}
        legacy = {'space_id': 'sp_test', 'kind': 'direct_artifact', 'rank': 1, 'created_at': captured['created_at']}
        answer = {'answer_id': 'ans_test', 'space_id': 'sp_test', 'evidence': [captured, legacy]}
        transport = FakeTransport()
        transport.responses = [json_response(answer)]
        result = SwarmClient(base_url='https://synthetic.invalid', transport=transport).get_space_answer('sp_test', 'ans_test')
        self.assertEqual(result, answer)
        self.assertNotIn('snapshot_version', result['evidence'][1])
        self.assertNotIn('artifact_sha256', result['evidence'][1])

    def test_server_provenance_and_declared_coverage_are_preserved(self):
        view = {'meta': {}, 'state_key': 'projection.ready', 'source_resource_ref': 'artifact:art_source', 'source_revision': 1,
                'coverage': {'version': 'context_coverage_v1', 'scope': 'declared_scope', 'status': 'complete_for_declared_scope',
                             'excerpt_only_count': 0, 'budget_omitted_count': 0, 'required_omitted_count': 0, 'rendered_bytes': 100},
                'citations': [{'resource_ref': 'artifact:art_source', 'rank': 1, 'source_version': 'source-revision',
                               'source_binding_hash': 'binding-revision', 'excerpt_hash': 'excerpt-revision',
                               'required': True, 'full_content_required': True, 'reason_key': 'explicit'}]}
        transport = FakeTransport()
        transport.responses = [json_response(view)]
        result = SwarmClient(base_url='https://synthetic.invalid', transport=transport).preview_handoff('sp_test', {'source_resource_ref': view['source_resource_ref']})
        self.assertEqual(result, view)

    def test_older_missing_fields_are_not_synthesized_as_complete(self):
        view = {'meta': {}, 'state_key': 'projection.ready', 'source_resource_ref': 'artifact:art_source', 'source_revision': 1,
                'citations': [{'resource_ref': 'artifact:art_source', 'rank': 1}]}
        transport = FakeTransport()
        transport.responses = [json_response(view)]
        result = SwarmClient(base_url='https://synthetic.invalid', transport=transport).preview_handoff('sp_test', {'source_resource_ref': view['source_resource_ref']})
        self.assertEqual(result, view)
        self.assertNotIn('coverage', result)
        self.assertNotIn('source_version', result['citations'][0])
