# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7a6b9523-7bc5-47de-9f83-1ac745e5820f",
# META       "default_lakehouse_name": "CP_gold_lakhoue",
# META       "default_lakehouse_workspace_id": "527fe858-1cca-4c4f-9ab8-d09f43eb5b5f",
# META       "known_lakehouses": [
# META         {
# META           "id": "7a6b9523-7bc5-47de-9f83-1ac745e5820f"
# META         },
# META         {
# META           "id": "68b18c8c-0335-4348-b250-58bb8892430f"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Incremental Load In Sementic Model

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark import StorageLevel
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# SILVER LAKEHOUSE

SILVER_TRADES = "CP_silver_lakhouse.dbo.trades"
SILVER_OHLC = "CP_silver_lakhouse.dbo.ohlc"
SILVER_TICKERS = "CP_silver_lakhouse.dbo.tickers"


# GOLD LAKEHOUSE

GOLD_TRADES = "fact_trades"
GOLD_OHLC = "fact_ohlc"
GOLD_TICKERS = "fact_tickers"


# DIMENSION TABLE

DIM_SYMBOL = "dim_symbol"


# CONTROL TABLE

CONTROL_TABLE = "ETL_Control"


# INCREMENTAL SETTINGS

OVERLAP_MINUTES = 2

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CONTROL_TABLE}
(
    TableName STRING,
    LastProcessedTime TIMESTAMP
)
USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
MERGE INTO {CONTROL_TABLE} AS target

USING (

    SELECT
        'trades' AS TableName,
        TIMESTAMP('1970-01-01 00:00:00') AS LastProcessedTime

    UNION ALL

    SELECT
        'ohlc',
        TIMESTAMP('1970-01-01 00:00:00')

    UNION ALL

    SELECT
        'tickers',
        TIMESTAMP('1970-01-01 00:00:00')

) AS source

ON target.TableName = source.TableName

WHEN NOT MATCHED THEN

INSERT (
    TableName,
    LastProcessedTime
)

VALUES (
    source.TableName,
    source.LastProcessedTime
)
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read dimension table

dim_symbol_df = (
    spark.read
    .table(DIM_SYMBOL)
    .select("symbol", "symbol_id")
)


# Check duplicate symbols

duplicate_symbols = (
    dim_symbol_df
    .groupBy("symbol")
    .count()
    .filter(F.col("count") > 1)
)


if duplicate_symbols.limit(1).count() > 0:

    display(duplicate_symbols)

    raise ValueError(
        "Duplicate symbols found in dim_symbol."
    )


print("Symbol dimension validation successful.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read Silver tables

silver_trades_df = spark.read.table(SILVER_TRADES)

silver_ohlc_df = spark.read.table(SILVER_OHLC)

silver_tickers_df = spark.read.table(SILVER_TICKERS)


# Validate timestamp column

for table_name, df in [

    ("trades", silver_trades_df),
    ("ohlc", silver_ohlc_df),
    ("tickers", silver_tickers_df)

]:

    if "timestamp" not in df.columns:

        raise ValueError(
            f"{table_name}: timestamp column is missing."
        )


# Combine distinct symbols

all_silver_symbols = (

    silver_trades_df.select("symbol")

    .unionByName(
        silver_ohlc_df.select("symbol")
    )

    .unionByName(
        silver_tickers_df.select("symbol")
    )

    .distinct()

)


# Find missing symbols

missing_symbols = (

    all_silver_symbols

    .join(
        dim_symbol_df,
        "symbol",
        "left_anti"
    )

)


if missing_symbols.limit(1).count() > 0:

    display(missing_symbols)

    raise ValueError(
        "Silver symbols missing from dim_symbol."
    )


print("All Silver tables validated successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def incremental_load(
    source_df,
    target_table,
    table_name,
    business_keys,
    last_watermark,
    overlap_minutes=2
):

    print(f"\nStarting incremental load: {table_name}")


    # 1. Filter incremental records

    incremental_df = source_df.filter(

        F.col("_watermark_ts").isNotNull() &

        (
            F.col("_watermark_ts") >

            (
                F.lit(last_watermark) -

                F.expr(
                    f"INTERVAL {overlap_minutes} MINUTES"
                )
            )
        )

    )


    # 2. Remove duplicate records

    window = (

        Window

        .partitionBy(*business_keys)

        .orderBy(
            F.col("_watermark_ts").desc()
        )

    )


    ranked_df = (

        incremental_df

        .withColumn(
            "_row_number",
            F.row_number().over(window)
        )

        .persist(StorageLevel.MEMORY_AND_DISK)

    )


    try:

        # 3. Calculate metrics in one aggregation

        metrics = (

            ranked_df

            .agg(

                F.count("*").alias("SourceRecords"),

                F.sum(

                    F.when(
                        F.col("_row_number") == 1,
                        1
                    ).otherwise(0)

                ).alias("DeduplicatedRecords"),

                F.max(

                    F.when(
                        F.col("_row_number") == 1,
                        F.col("_watermark_ts")
                    )

                ).alias("NewWatermark")

            )

            .first()

        )


        source_count = int(
            metrics["SourceRecords"]
        )

        deduplicated_count = int(
            metrics["DeduplicatedRecords"] or 0
        )

        new_watermark = metrics["NewWatermark"]


        # 4. Handle empty data

        if source_count == 0:

            print(f"No new records: {table_name}")

            return {

                "Table": table_name,
                "SourceRecords": 0,
                "Inserted": 0,
                "Updated": 0,
                "Duplicates": 0,
                "Skipped": 0,
                "NewWatermark": None,
                "Status": "No new data"

            }


        duplicates = (
            source_count - deduplicated_count
        )


        # 5. Prepare deduplicated data

        deduplicated_df = (

            ranked_df

            .filter(
                F.col("_row_number") == 1
            )

            .drop("_row_number")

        )


        # 6. Read target schema

        target_schema = spark.table(
            target_table
        ).schema


        target_columns = {
            field.name
            for field in target_schema
        }


        # Validate business keys

        for key in business_keys:

            if key not in deduplicated_df.columns:

                raise ValueError(
                    f"Business key {key} missing in {table_name}"
                )

            if key not in target_columns:

                raise ValueError(
                    f"Business key {key} missing in {target_table}"
                )


        # 7. Align source with target columns

        insert_columns = [

            column

            for column in deduplicated_df.columns

            if (

                column != "_watermark_ts"

                and column in target_columns

            )

        ]


        merge_source_df = deduplicated_df.select(
            *insert_columns
        )


        # 8. Create merge condition

        merge_condition = " AND ".join(

            [

                f"t.`{key}` <=> s.`{key}`"

                for key in business_keys

            ]

        )


        # 9. Create insert mapping

        insert_mapping = {

            column: f"s.`{column}`"

            for column in insert_columns

        }


        # 10. Execute Delta MERGE

        target = DeltaTable.forName(
            spark,
            target_table
        )


        (

            target.alias("t")

            .merge(
                merge_source_df.alias("s"),
                merge_condition
            )

            .whenNotMatchedInsert(
                values=insert_mapping
            )

            .execute()

        )


        # 11. Retrieve actual Delta metrics

        operation_metrics = (

            target.history(1)

            .select("operationMetrics")

            .first()["operationMetrics"]

        )


        inserted = int(

            operation_metrics.get(
                "numTargetRowsInserted", 0
            )

        )


        updated = int(

            operation_metrics.get(
                "numTargetRowsUpdated", 0
            )

        )


        skipped = (

            deduplicated_count

            - inserted

            - updated

        )


        # 12. Return results

        print(f"Completed: {table_name}")

        print(f"Source records: {source_count}")

        print(f"Inserted: {inserted}")

        print(f"Duplicates: {duplicates}")

        print(f"Already existing: {skipped}")


        return {

            "Table": table_name,

            "SourceRecords": source_count,

            "Inserted": inserted,

            "Updated": updated,

            "Duplicates": duplicates,

            "Skipped": skipped,

            "NewWatermark": new_watermark,

            "Status": "Success"

        }


    finally:

        ranked_df.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

trades_gold_df = (

    silver_trades_df

    .join(
        dim_symbol_df,
        "symbol",
        "inner"
    )

    .withColumn(
        "_watermark_ts",
        F.to_timestamp("timestamp")
    )

    .select(

        F.col("symbol_id").cast("int"),

        "symbol",
        "side",
        "price",
        "qty",
        "order_type",
        "trade_id",
        "timestamp",
        "_watermark_ts"

    )

)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ohlc_gold_df = (

    silver_ohlc_df

    .join(
        dim_symbol_df,
        "symbol",
        "inner"
    )

    .withColumn(
        "_watermark_ts",
        F.to_timestamp("timestamp")
    )

    .select(

        F.col("symbol_id").cast("int"),

        "symbol",
        "open",
        "high",
        "low",
        "close",
        "trades",
        "volume",
        "vwap",
        "interval_begin",
        "interval",
        "timestamp",
        "_watermark_ts"

    )

)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tickers_gold_df = (

    silver_tickers_df

    .join(
        dim_symbol_df,
        "symbol",
        "inner"
    )

    .withColumn(
        "_watermark_ts",
        F.to_timestamp("timestamp")
    )

    .select(

        F.col("symbol_id").cast("int"),

        "symbol",
        "bid",
        "bid_qty",
        "ask",
        "ask_qty",
        "last",
        "volume",
        "vwap",
        "low",
        "high",
        "change",
        "change_pct",
        "trades",
        "timestamp",
        "_watermark_ts"

    )

)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================
# 1. READ WATERMARKS ONCE
# =====================================

watermark_rows = (

    spark.table(CONTROL_TABLE)

    .collect()

)


watermarks = {

    row["TableName"]: row["LastProcessedTime"]

    for row in watermark_rows

}


required_tables = {"trades", "ohlc", "tickers"}

if not required_tables.issubset(watermarks):

    raise ValueError(
        "One or more watermark entries are missing from ETL_Control."
    )


# =====================================
# 2. EXECUTE THREE INCREMENTAL LOADS
# =====================================

results = []


# TRADES

trades_result = incremental_load(

    source_df=trades_gold_df,

    target_table=GOLD_TRADES,

    table_name="trades",

    business_keys=[
        "symbol",
        "trade_id"
    ],

    last_watermark=watermarks["trades"],

    overlap_minutes=OVERLAP_MINUTES

)

results.append(trades_result)


# OHLC

ohlc_result = incremental_load(

    source_df=ohlc_gold_df,

    target_table=GOLD_OHLC,

    table_name="ohlc",

    business_keys=[
        "symbol",
        "interval",
        "interval_begin",
        "timestamp"
    ],

    last_watermark=watermarks["ohlc"],

    overlap_minutes=OVERLAP_MINUTES

)

results.append(ohlc_result)


# TICKERS

tickers_result = incremental_load(

    source_df=tickers_gold_df,

    target_table=GOLD_TICKERS,

    table_name="tickers",

    business_keys=[
        "symbol",
        "timestamp"
    ],

    last_watermark=watermarks["tickers"],

    overlap_minutes=OVERLAP_MINUTES

)

results.append(tickers_result)


# =====================================
# 3. COLLECT SUCCESSFUL WATERMARKS
# =====================================

pending_watermarks = [

    (
        result["Table"],
        result["NewWatermark"]
    )

    for result in results

    if result["NewWatermark"] is not None

]


# =====================================
# 4. UPDATE CONTROL TABLE ONCE
# =====================================

if pending_watermarks:

    watermark_updates_df = spark.createDataFrame(

        pending_watermarks,

        schema="""
            TableName STRING,
            LastProcessedTime TIMESTAMP
        """

    )


    control = DeltaTable.forName(
        spark,
        CONTROL_TABLE
    )


    (

        control.alias("t")

        .merge(

            watermark_updates_df.alias("s"),

            "t.TableName = s.TableName"

        )

        .whenMatchedUpdate(

            set={

                "LastProcessedTime": """

                    greatest(
                        t.LastProcessedTime,
                        s.LastProcessedTime
                    )

                """

            }

        )

        .execute()

    )


print("All incremental loads completed successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

report_schema = StructType([

    StructField("Table", StringType(), False),

    StructField("SourceRecords", LongType(), False),

    StructField("Inserted", LongType(), False),

    StructField("Updated", LongType(), False),

    StructField("Duplicates", LongType(), False),

    StructField("Skipped", LongType(), False),

    StructField("NewWatermark", TimestampType(), True),

    StructField("Status", StringType(), False)

])


report_df = spark.createDataFrame(
    results,
    schema=report_schema
)


display(report_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary_df = report_df.agg(

    F.sum("SourceRecords").alias(
        "Total_Source_Records"
    ),

    F.sum("Inserted").alias(
        "Total_Inserted"
    ),

    F.sum("Updated").alias(
        "Total_Updated"
    ),

    F.sum("Duplicates").alias(
        "Total_Duplicates"
    ),

    F.sum("Skipped").alias(
        "Total_Skipped"
    )

)


display(summary_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(

    spark.table(CONTROL_TABLE)

    .orderBy("TableName")

)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
