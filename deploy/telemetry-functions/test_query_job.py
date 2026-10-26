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
    def test_http_error_carries_status_and_loki_message(self):
        e = urllib.error.HTTPError("http://loki", 413, "Request Entity Too Large", {},
                                   io.BytesIO(b"response larger than the max message size\n"))
        self.assertEqual(query_job.refusal_reason(e),
                         "HTTP 413 Request Entity Too Large: response larger than the max message size")

    def test_http_error_without_body(self):
        e = urllib.error.HTTPError("http://loki", 503, "Service Unavailable", {}, io.BytesIO(b""))
        self.assertEqual(query_job.refusal_reason(e), "HTTP 503 Service Unavailable")

    def test_other_errors_name_their_type(self):
        self.assertEqual(query_job.refusal_reason(KeyError("data")), "KeyError: 'data'")
        self.assertEqual(query_job.refusal_reason(ValueError("no result from loki-raw")),
                         "ValueError: no result from loki-raw")


if __name__ == "__main__":
    unittest.main()
