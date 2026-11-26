import unittest

try:
    from pyspark.sql import SparkSession
    from catalog_etl.transform import build_items
except ImportError:
    SparkSession = None


@unittest.skipIf(SparkSession is None, "pyspark is not installed")
class TransformTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_filters_and_load_date(self):
        current = "2999-12-31 00:00:00"
        rows = [
            (1, "a", "G1", 0, current),
            (2, "b", "G1", 1, current),
            (3, "c", "G3", 0, current),
            (4, "d", "G1", 0, "2020-01-01 00:00:00"),
        ]
        df = self.spark.createDataFrame(rows, ["item_id", "item_name", "group_code", "tech_flg", "valid_to_dttm"])
        result = build_items(df, "2026-01-15").collect()
        self.assertEqual([r.item_id for r in result], [1])
        self.assertEqual(str(result[0].load_date), "2026-01-15")


if __name__ == "__main__":
    unittest.main()
