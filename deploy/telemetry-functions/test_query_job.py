"""Unit tests for query_job's comparison rules: python3 -m unittest deploy/telemetry-functions/test_query_job.py"""
import io
import os
import sys
import unittest
import urllib.error

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import query_job


class IncompleteMethods(unittest.TestCase):
    def test_equal_points_are_all_comparable(self):
        self.assertEqual(query_job.incomplete_methods({"sketch": 9_600_000, "exact": 9_600_000}), set())

    def test_small_difference_is_comparable(self):
        self.assertEqual(query_job.incomplete_methods({"sketch": 9_598_988, "exact": 9_600_000}), set())

    def test_exact_that_lost_a_source_is_left_out(self):
        self.assertEqual(query_job.incomplete_methods({"sketch": 9_600_000, "exact": 4_735_189}), {"exact"})

    def test_sketch_that_lost_a_source_is_left_out(self):
        self.assertEqual(query_job.incomplete_methods({"sketch": 4_800_000, "exact": 9_600_000}), {"sketch"})

    def test_missing_points_judge_nothing(self):
        self.assertEqual(query_job.incomplete_methods({"sketch": 0.0, "exact": 9_600_000}), set())
        self.assertEqual(query_job.incomplete_methods({"sketch": 9_600_000, "exact": 0.0}), set())


class RefusalReason(unittest.TestCase):
    def test_http_error_carries_status_and_backend_message(self):
        e = urllib.error.HTTPError("http://prometheus-raw", 413, "Request Entity Too Large", {},
                                   io.BytesIO(b"response larger than the max message size\n"))
        self.assertEqual(query_job.refusal_reason(e),
                         "HTTP 413 Request Entity Too Large: response larger than the max message size")

    def test_http_error_without_body(self):
        e = urllib.error.HTTPError("http://prometheus-raw", 503, "Service Unavailable", {}, io.BytesIO(b""))
        self.assertEqual(query_job.refusal_reason(e), "HTTP 503 Service Unavailable")

    def test_other_errors_name_their_type(self):
        self.assertEqual(query_job.refusal_reason(KeyError("data")), "KeyError: 'data'")
        self.assertEqual(query_job.refusal_reason(ValueError("no result from prometheus-raw")),
                         "ValueError: no result from prometheus-raw")


class RawEvents(unittest.TestCase):
    def setUp(self):
        query_job.samples.clear()

    def events(self):
        return [dict(labels) for (name, labels) in query_job.samples if name == "asap_event"]

    def test_new_heavy_hitter_after_three_windows(self):
        ev = query_job.RawEvents()
        for end in (60, 120, 180):
            self.assertEqual(ev.novelty([("/a", 10.0), ("/b", 5.0)], end), [])
        self.assertEqual(ev.novelty([("/new", 20.0), ("/a", 10.0)], 240), [(1, "/new", 20.0)])
        self.assertEqual([e["key"] for e in self.events()], ["/new"])
        # Seen in the baseline now: not new again.
        self.assertEqual(ev.novelty([("/new", 20.0)], 300), [])

    def test_cardinality_classes_and_changes(self):
        ev = query_job.RawEvents()
        self.assertEqual(ev.cardinality("user.id", 950, 1000, 60), "identifier-like")
        self.assertIsNone(ev.cardinality("user.id", 9, 50, 120), "below min_points")
        for end in (60, 120, 180):
            self.assertEqual(ev.cardinality("k8s.pod.name", 50, 1000, end), "stable")
        self.assertEqual(ev.cardinality("k8s.pod.name", 61, 1000, 240), "growing")
        changes = sorted((e["attribute_key"], e["state"]) for e in self.events())
        self.assertEqual(changes, [("k8s.pod.name", "growing"), ("k8s.pod.name", "stable"),
                                   ("user.id", "identifier-like")])


if __name__ == "__main__":
    unittest.main()
