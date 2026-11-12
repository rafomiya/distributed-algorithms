import json
from pathlib import Path
import tempfile
import unittest

from scripts.verify import verify


class VerificationTest(unittest.TestCase):
    def check_events(self, events):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            path.write_text("\n".join(json.dumps({"mono_ns": index, **event})
                                      for index, event in enumerate(events)))
            return verify(directory)

    def test_rejects_overlapping_computer_critical_sections(self):
        with self.assertRaisesRegex(AssertionError, "sobrepostas"):
            self.check_events([
                {"event": "cs_enter", "node": "node1", "resource": "printer1", "request_id": "a"},
                {"event": "cs_enter", "node": "node2", "resource": "printer1", "request_id": "b"},
            ])

    def test_rejects_printer_collision_even_without_physical_overlap(self):
        with self.assertRaises(AssertionError):
            self.check_events([{"event": "overlap_violation", "node": "printer1"}])

    def test_reports_crashed_holder_as_open_interval(self):
        result = self.check_events([
            {"event": "cs_enter", "node": "node1", "resource": "printer1", "request_id": "a"},
        ])
        self.assertEqual(result["open_cs"], ["printer1"])
