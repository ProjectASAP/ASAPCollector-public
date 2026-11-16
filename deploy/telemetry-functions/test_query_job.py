"""Unit tests for query_job's comparison rules: python3 -m unittest deploy/telemetry-functions/test_query_job.py"""
import io
import re
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


class Timing(unittest.TestCase):
    def test_points_wait_for_their_window_then_the_query(self):
        query_job.samples.clear()
        query_job.report_timing("exact", 120, {0.5: 11.0, 0.99: 11.0}, 2.0)
        got = {(name, dict(labels)["quantile"]): v for (name, labels), (v, _) in query_job.samples.items()}
        self.assertEqual(got[("asap_data_available_seconds", "0.5")], 41.0)
        self.assertAlmostEqual(got[("asap_data_available_seconds", "0.99")], 70.4)
        self.assertEqual(got[("asap_result_age_seconds", "0.5")], 43.0)

    def test_no_query_no_result_age(self):
        query_job.samples.clear()
        query_job.report_timing("raw", 120, {0.5: 0.01, 0.99: 0.02}, None, window_wait=False)
        self.assertEqual({name for name, _ in query_job.samples},
                         {"asap_window_wait_seconds", "asap_data_available_seconds"})

    def test_raw_has_no_window_wait(self):
        query_job.samples.clear()
        query_job.report_timing("raw", 120, {0.5: 0.005, 0.99: 0.03}, 17.0, window_wait=False)
        got = {(name, dict(labels)["quantile"]): v for (name, labels), (v, _) in query_job.samples.items()}
        self.assertEqual(got[("asap_window_wait_seconds", "0.5")], 0.0)
        self.assertEqual(got[("asap_result_age_seconds", "0.5")], 17.005)


class RulesMatchDetectorConfigs(unittest.TestCase):
    def test_raw_event_rules_are_the_detectors(self):
        here = os.path.dirname(os.path.abspath(__file__))
        card = open(os.path.join(here, "cardinality", "detector.yaml")).read()
        rules = dict(re.findall(r"(\w+): ([\d.]+)", re.search(r"cardinality: \{([^}]*)\}", card)[1]))
        self.assertEqual((float(rules["identifier_ratio"]), float(rules["growth"]), int(rules["baseline_windows"])),
                         (query_job.IDENTIFIER_RATIO, query_job.GROWTH, query_job.CLASS_BASELINE_WINDOWS))
        topk = open(os.path.join(here, "topk", "detector.yaml")).read()
        self.assertEqual(int(re.search(r"novelty_windows: (\d+)", topk)[1]), query_job.NOVELTY_WINDOWS)


if __name__ == "__main__":
    unittest.main()
