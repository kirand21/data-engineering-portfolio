import argparse
import os

from pyspark.sql import SparkSession

from catalog_etl.config import jdbc_options_from_env, validate_run_date
from catalog_etl.transform import build_items


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--target-table", required=True)
    parser.add_argument("--run-date", required=True)
    args = parser.parse_args()
    run_date = validate_run_date(args.run_date)

    spark = SparkSession.builder.appName("catalog_items_load").enableHiveSupport().getOrCreate()
    try:
        result = build_items(spark.table(args.source_table), run_date)
        (result.write.format("jdbc")
            .options(**jdbc_options_from_env(os.environ, args.target_table))
            .mode("append")
            .save())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
