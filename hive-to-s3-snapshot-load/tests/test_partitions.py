import unittest
from datetime import datetime

from price_load.partitions import find_partition_prefix, partition_for


class PartitionTests(unittest.TestCase):
    def test_previous_day(self):
        self.assertEqual(partition_for(datetime(2026, 3, 1, 7, 20)), "2026-02-28")

    def test_requires_date(self):
        with self.assertRaises(ValueError):
            partition_for(None)

    def test_missing_partition(self):
        with self.assertRaises(FileNotFoundError):
            find_partition_prefix("b", "p", "2026-01-01", [])

    def test_prefix(self):
        keys = ["p/dataflow_date=2026-01-01/part-0.orc"]
        self.assertEqual(find_partition_prefix("b", "p", "2026-01-01", keys),
                         "s3a://b/p/dataflow_date=2026-01-01/")

    def test_ignores_non_data_files(self):
        with self.assertRaises(FileNotFoundError):
            find_partition_prefix("b", "p", "2026-01-01", ["p/dataflow_date=2026-01-01/_SUCCESS"])

    def test_bad_date(self):
        with self.assertRaises(ValueError):
            find_partition_prefix("b", "p", "01.01.2026", [])


if __name__ == "__main__":
    unittest.main()
