import argparse
import os

from pyspark.sql import SparkSession

from price_load.config import jdbc_options_from_env, validate_date
from price_load.transform import assert_valid, normalize_prices


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-path", required=True)
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--target-table", required=True)
    args = parser.parse_args()
    snapshot_date = validate_date(args.snapshot_date)

    spark = SparkSession.builder.appName("current_prices_from_s3").getOrCreate()
    try:
        raw = spark.read.orc(args.source_path)
        if not raw.limit(1).count():
            raise ValueError("Snapshot partition is empty")
        prices = normalize_prices(raw, snapshot_date).cache()
        assert_valid(prices)
        (prices.write.format("jdbc")
            .options(**jdbc_options_from_env(os.environ, args.target_table))
            .mode("append")
            .save())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
