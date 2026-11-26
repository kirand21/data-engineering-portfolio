import unittest

try:
    from pyspark.sql import SparkSession
    from price_load.transform import assert_valid, normalize_prices
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

    def frame(self, rows):
        df = self.spark.createDataFrame(rows, ["item_id", "shop_id", "price"])
        return normalize_prices(df, "2026-01-15")

    def test_valid_snapshot(self):
        assert_valid(self.frame([(" 1 ", "10", "12.5"), ("2", "10", "3")]))

    def test_duplicates_are_rejected(self):
        with self.assertRaises(ValueError):
            assert_valid(self.frame([("1", "10", "1"), ("1", "10", "2")]))

    def test_negative_or_bad_price_rejected(self):
        for price in ("-1", "abc"):
            with self.assertRaises(ValueError):
                assert_valid(self.frame([("1", "10", price)]))

    def test_blank_key_rejected(self):
        with self.assertRaises(ValueError):
            assert_valid(self.frame([("  ", "10", "1")]))


if __name__ == "__main__":
    unittest.main()
