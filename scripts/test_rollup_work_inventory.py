import json
import unittest

from rollup_work_inventory import EVENT, MAX_DECODED_BYTES, inventory


class InventoryTests(unittest.TestCase):
    def test_formats_boundaries_and_missing_evidence(self):
        for size, shards in [(0, 1), (MAX_DECODED_BYTES, 1), (MAX_DECODED_BYTES + 1, 2)]:
            fields = dict(event=EVENT, table="metrics", operation="BaseRollup", estimated_decoded_bytes=size)
            lines = [json.dumps(fields), json.dumps({"fields": fields}),
                     '\x1b[32mINFO\x1b[0m ' + ' '.join(f'{k}={json.dumps(v)}' for k, v in fields.items())]
            for line in lines:
                with self.subTest(size=size, line=line):
                    report = inventory([line, line, 'event="maintenance_rollup_slow_unit"'])
                    self.assertEqual(report["publication_observations"], 2)
                    self.assertEqual(report["groups"], [dict(table="metrics", operation="BaseRollup", inferred_shards=shards,
                        shard_evidence="estimated", publications=2, estimated_input_bytes=2 * size,
                        estimated_input_bytes_times_shards=2 * size * shards)])
        for value in [None, -1, True, 1.2, "bad"]:
            with self.subTest(value=value):
                report = inventory([json.dumps(dict(fields, estimated_decoded_bytes=value))])
                self.assertEqual((report["publications_missing_evidence"], report["groups"]), (1, []))

    def test_capacity_and_unrelated_records(self):
        with self.assertRaises(ValueError):
            inventory([], 0)
        self.assertEqual(inventory(['not json', '{}', '[]'])["publication_observations"], 0)
        self.assertEqual(inventory([])["groups"], [])

    def test_recorded_shards_override_estimates_without_merging_evidence(self):
        fields = dict(event=EVENT, table="metrics", operation="BaseRollup", estimated_decoded_bytes=MAX_DECODED_BYTES + 1)
        report = inventory([json.dumps(fields), json.dumps(dict(fields, hash_shards=2)),
                            json.dumps(dict(fields, hash_shards="6"))])
        self.assertEqual([(g["shard_evidence"], g.get("recorded_shards", g.get("inferred_shards")), g["publications"])
                          for g in report["groups"]], [("estimated", 2, 1), ("recorded", 2, 1), ("recorded", 6, 1)])
        self.assertEqual(report["groups"][-1]["estimated_input_bytes_times_shards"], 6 * (MAX_DECODED_BYTES + 1))
        for invalid in [None, 0, -1, True, 1.2, "bad"]:
            with self.subTest(invalid=invalid):
                report = inventory([json.dumps(dict(fields, hash_shards=invalid))])
                self.assertEqual((report["publications_missing_evidence"], report["groups"]), (1, []))

    def test_shard_bands_preserve_byte_weights_and_evidence(self):
        fields = dict(event=EVENT, table="metrics", operation="BaseRollup", estimated_decoded_bytes=100)
        lines = [json.dumps(dict(fields, hash_shards=shards)) for shards in [1, 2, 3, 4, 5, 8, 9]]
        # A second tier contributes to the same evidence band, but not the base subtotal.
        lines.append(json.dumps(dict(fields, table="dashboard", operation="DerivedRollup", hash_shards=2, estimated_decoded_bytes=50)))
        lines.append(json.dumps(fields))
        report = inventory(lines)
        self.assertEqual(report["shard_bands"], [
            dict(shard_evidence="estimated", shard_band="1", publications=1, estimated_input_bytes=100,
                 estimated_input_bytes_times_shards=100),
            *[dict(shard_evidence="recorded", shard_band=band, publications=count,
                   estimated_input_bytes=size, estimated_input_bytes_times_shards=weighted)
              for band, count, size, weighted in [("1", 1, 100, 100), ("2", 2, 150, 300),
                                                  ("3–4", 2, 200, 700), ("5–8", 2, 200, 1300), (">8", 1, 100, 900)]],
        ])
        self.assertEqual(report["exposure"], [
            dict(shard_evidence="estimated", publications=1, estimated_input_bytes=100,
                 estimated_input_bytes_times_shards=100, multishard_estimated_input_bytes=0,
                 multishard_input_fraction=0.0, estimated_pass_multiplier=1.0),
            dict(shard_evidence="recorded", publications=8, estimated_input_bytes=750,
                 estimated_input_bytes_times_shards=3300, multishard_estimated_input_bytes=650,
                 multishard_input_fraction=650 / 750, estimated_pass_multiplier=3300 / 750),
        ])

    def test_empty_input_has_no_weighted_ratio(self):
        fields = dict(event=EVENT, table="metrics", operation="BaseRollup", estimated_decoded_bytes=0, hash_shards=1)
        self.assertEqual(inventory([])["exposure"], [])
        exposure = inventory([json.dumps(fields)])["exposure"][0]
        self.assertIsNone(exposure["multishard_input_fraction"])
        self.assertIsNone(exposure["estimated_pass_multiplier"])


if __name__ == "__main__":
    unittest.main()
