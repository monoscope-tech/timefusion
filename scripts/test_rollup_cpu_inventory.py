import unittest

from rollup_cpu_inventory import covered_samples, summarize


class CpuInventoryTests(unittest.TestCase):
    def test_interval_union(self):
        for intervals, expected in [([], 0), ([(2, 5)], 3), ([(5, 8), (0, 5)], 8),
                                    ([(0, 8), (2, 4), (0, 8), (10, 12)], 10)]:
            with self.subTest(intervals=intervals):
                self.assertEqual(covered_samples(intervals), expected)

    def test_inferno_frames_count_nested_matches_once(self):
        def graph(frames):
            body = "".join(
                f'<g><title>{name} (samples)</title><rect fg:x="{lo}" fg:w="{width}"/></g>'
                for name, lo, width in frames
            )
            return (f'<svg xmlns="http://www.w3.org/2000/svg" '
                    f'xmlns:fg="http://github.com/jonhoo/inferno">{body}</svg>')

        frames = [("all", 0, 100), ("run_coordinator_rollup_selected", 10, 30),
                  ("run_coordinator_rollup_selected", 15, 5), ("parquet::decode", 20, 10),
                  ("run_coordinator_rollup_selected", 70, 10)]
        result = summarize(graph(frames))
        self.assertEqual(result["samples"], 100)
        self.assertEqual(result["inclusive_samples"]["rollup_caller"], 40)
        self.assertEqual(result["inclusive_samples"]["parquet"], 10)
        for invalid in [[], [("all", 0, 0)], frames + [("bad", 99, 2)],
                        frames + [("bad", -1, 2)], frames + [("all", 0, 100)]]:
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                summarize(graph(invalid))


if __name__ == "__main__":
    unittest.main()
