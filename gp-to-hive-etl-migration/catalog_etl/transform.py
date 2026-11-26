from pyspark.sql import functions as F

CURRENT_VALID_TO = "2999-12-31 00:00:00"
EXCLUDED_GROUPS = ("G3", "G7")
NULL_COLUMNS = ("legacy_flag", "legacy_code")


def build_items(df, run_date):
    current = df.filter(F.col("valid_to_dttm") == F.lit(CURRENT_VALID_TO))
    technical = current.filter(F.col("tech_flg") == 1).select("item_id").distinct()
    items = current.join(technical, "item_id", "left_anti")
    items = items.filter(~F.col("group_code").isin(*EXCLUDED_GROUPS))
    for column in NULL_COLUMNS:
        items = items.withColumn(column, F.lit(None).cast("string"))
    items = items.withColumn("load_date", F.lit(run_date).cast("date"))
    return items.select("item_id", "item_name", "group_code", *NULL_COLUMNS, "load_date")
