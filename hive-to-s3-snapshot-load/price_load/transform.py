from pyspark.sql import functions as F

KEY_COLUMNS = ["item_id", "shop_id"]


def normalize_prices(df, snapshot_date):
    return df.select(
        F.trim(F.col("item_id").cast("string")).alias("item_id"),
        F.trim(F.col("shop_id").cast("string")).alias("shop_id"),
        F.col("price").cast("decimal(18,2)").alias("price"),
        F.lit(snapshot_date).cast("date").alias("load_date"),
    )


def find_problems(df):
    """Counts rows that must not be loaded. Nothing is silently dropped."""
    invalid_keys = df.filter(
        F.col("item_id").isNull() | (F.col("item_id") == "")
        | F.col("shop_id").isNull() | (F.col("shop_id") == "")
    ).count()
    invalid_prices = df.filter(F.col("price").isNull() | (F.col("price") < 0)).count()
    duplicate_keys = df.groupBy(*KEY_COLUMNS).count().filter(F.col("count") > 1).count()
    return {"invalid_keys": invalid_keys, "invalid_prices": invalid_prices,
            "duplicate_keys": duplicate_keys}


def assert_valid(df):
    problems = find_problems(df)
    if any(problems.values()):
        raise ValueError(f"Snapshot validation failed: {problems}")
