"""Unit tests for query_job's comparison rules: python3 -m unittest deploy/telemetry-functions/test_query_job.py"""
import io
import math
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


class HeavyHitterAccuracy(unittest.TestCase):
    # A measured window: 100,000 points, phi 1%, Count-Min 4 x 512, so an
    # estimate is at most e / 512 * 100,000 = 531 points above the true count.
    POINTS = 100_000.0
    BOUND = math.e / 512 * POINTS
    HEAVY = [(f"h{i}", 2_000.0) for i in range(9)]

    def test_key_just_under_phi_is_within_the_bound(self):
        # route-9 and route-8 have 0.958% and 0.974% of the points; Count-Min
        # estimates them at 1.004% and 1.002%.
        exact = self.HEAVY + [("route-9", 958.0), ("route-8", 974.0), ("light", 400.0)]
        reported = self.HEAVY + [("route-9", 1_004.0), ("route-8", 1_002.0)]
        acc = query_job.heavy_hitter_accuracy(self.POINTS, exact, reported, self.BOUND)
        self.assertEqual((acc["precision"], acc["recall"]), (1.0, 1.0))
        # Without a bound (raw, exact counts) they are false positives: 9 / 11.
        strict = query_job.heavy_hitter_accuracy(self.POINTS, exact, reported)
        self.assertAlmostEqual(strict["precision"], 9 / 11)

    def test_key_far_under_phi_is_a_false_positive(self):
        # 0.4% of the points is below 1,000 - 531 = 469.
        exact = self.HEAVY + [("light", 400.0)]
        reported = self.HEAVY + [("light", 1_000.0)]
        acc = query_job.heavy_hitter_accuracy(self.POINTS, exact, reported, self.BOUND)
        self.assertAlmostEqual(acc["precision"], 9 / 10)


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
    def setUp(self):
        query_job.samples.clear()

    def got(self):
        return {name: v for (name, _), (v, _) in query_job.samples.items()}

    def test_every_approach_waits_for_the_range(self):
        query_job.report_timing("raw", 1200, 0.02, 30.0)
        self.assertEqual(self.got(), {"asap_window_wait_seconds": 300.0, "asap_data_available_seconds": 300.02,
                                      "asap_result_age_seconds": 330.02})

    def test_no_query_no_result_age(self):
        query_job.report_timing("raw", 1200, 0.01, None)
        self.assertNotIn("asap_result_age_seconds", self.got())


class RulesMatchDetectorConfigs(unittest.TestCase):
    def test_raw_event_rules_are_the_detectors(self):
        here = os.path.dirname(os.path.abspath(__file__))
        card = open(os.path.join(here, "cardinality", "detector.yaml")).read()
        rules = dict(re.findall(r"(\w+): ([\d.]+)", re.search(r"cardinality: \{([^}]*)\}", card)[1]))
        self.assertEqual((float(rules["identifier_ratio"]), float(rules["growth"]), int(rules["baseline_windows"])),
                         (query_job.IDENTIFIER_RATIO, query_job.GROWTH, query_job.CLASS_BASELINE_WINDOWS))
        topk = open(os.path.join(here, "topk", "detector.yaml")).read()
        self.assertEqual(int(re.search(r"novelty_windows: (\d+)", topk)[1]), query_job.NOVELTY_WINDOWS)
        for config in (card, topk):
            self.assertEqual(int(re.search(r"range_windows: (\d+)", config)[1]), query_job.RANGE_WINDOWS)


if __name__ == "__main__":
    unittest.main()
