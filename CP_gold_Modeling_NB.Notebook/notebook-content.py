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

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.window import Window


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

trades = spark.read.format('delta').load("abfss://CP_Crypto_Medallion_Architecture@onelake.dfs.fabric.microsoft.com/CP_silver_lakhouse.Lakehouse/Tables/dbo/Trades")
ohlc=spark.read.format('delta').load('abfss://CP_Crypto_Medallion_Architecture@onelake.dfs.fabric.microsoft.com/CP_silver_lakhouse.Lakehouse/Tables/dbo/ohlc')
tickers=spark.read.format('delta').load('abfss://CP_Crypto_Medallion_Architecture@onelake.dfs.fabric.microsoft.com/CP_silver_lakhouse.Lakehouse/Tables/dbo/Tickers')
symbol_df=spark.read.table('dim_symbol')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Creating dim_symbole

# CELL ********************

window=Window.partitionBy().orderBy('symbol')
df_symbol=ohlc.select(col('symbol')).distinct()
symbol_df=df_symbol.withColumn('symbol_id',row_number().over(window)).select(col('symbol_id'),'symbol')

symbol_df.write.format('delta').saveAsTable('dim_symbol')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Creating fact ohlc Table

# CELL ********************

df_joined=ohlc.join(symbol_df,on='symbol',how='inner').select(
    'symbol_id',
    'symbol',
    'open',
    'high',
    'low',
    'close',
    'trades',
    'volume',
    'vwap',
    'interval_begin',
    'interval',
    col('timestamp').cast('timestamp')

)
df_joined.write.format('delta').saveAsTable('fact_ohlc')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined_trades=trades.join(symbol_df,on='symbol',how='inner').select(
    'symbol_id',
    'symbol',
    'side',
    'price',
    'qty',
    'order_type',
    'trade_id',
    col('timestamp').cast('timestamp')

)
df_joined_trades.write.format('delta').saveAsTable('fact_trades')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined_tickers=tickers.join(symbol_df,on='symbol',how='inner').select(
    'symbol_id',
    'symbol',
    'bid',
    'bid_qty',
    'ask',
    'ask_qty',
    'last',
    'volume',
    'vwap',
    'low',
    'high',
    'change',
    'change_pct',
    'trades',
    col('timestamp').cast('timestamp')

)
df_joined_tickers.write.format('delta').saveAsTable('fact_tickers')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

START_DATE = "2024-01-01"
END_DATE = "2027-12-31"

date_df = spark.sql(f"""
    SELECT explode(
        sequence(
            to_date('{START_DATE}'),
            to_date('{END_DATE}'),
            interval 1 day
        )
    ) AS full_date
""")
date_df.write.format('delta').saveAsTable('dim_date')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

trades=spark.read.format('delta').table('fact_trades')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

trades.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # tHE ENd . . . !
