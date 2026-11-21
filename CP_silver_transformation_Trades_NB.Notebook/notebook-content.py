# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "68b18c8c-0335-4348-b250-58bb8892430f",
# META       "default_lakehouse_name": "CP_silver_lakhouse",
# META       "default_lakehouse_workspace_id": "527fe858-1cca-4c4f-9ab8-d09f43eb5b5f",
# META       "known_lakehouses": [
# META         {
# META           "id": "68b18c8c-0335-4348-b250-58bb8892430f"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "1d4a0b98-1873-bfd5-4279-66add989e8e6",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     },
# META     "mirrored_db": {
# META       "known_mirrored_dbs": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Transformating & Loading Trades Data in Silver Lakhos in Silver Layer 

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
from confluent_kafka import Consumer
from IPython.display import JSON
import configparser
import json
import logging 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

logger=logging.getLogger('silver_transformation')
logger.setLevel('DEBUG')

Rotating_logger=logging.handlers.RotatingFileHandler('/lakehouse/default/Files/crypto_silver_log/transformation_trades_log',maxBytes=102400000,backupCount=5)
Stream_handler=logging.StreamHandler()

Rotating_logger.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
Stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))

logger.addHandler(Rotating_logger)
logger.addHandler(Stream_handler)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

config=configparser.ConfigParser()
topic=configparser.ConfigParser()

config.read('/lakehouse/default/Files/silver_kafka_credentials/kafka_credentials.ini')

topic.read('/lakehouse/default/Files/silver_kafka_credentials/kafka_topics.ini')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

kafka_jaas_config = (
    'org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="{config['kafka']['KAFKA_API_KEY']}" '
    f'password="{config['kafka']['KAFKA_API_SECRET']}";'
)
df = spark.readStream.format('kafka')\
    .option('kafka.bootstrap.servers', config['kafka']['KAFKA_BOOTSTRAP_SERVERS'])\
    .option('subscribe', topic['kafka_topics']['KAFKA_TOPIC_TRADES'])\
    .option('startingOffsets', 'earliest')\
    .option('kafka.security.protocol',config['kafka']['KAFKA_SECURITY_PROTOCOL'])\
    .option('kafka.sasl.mechanism','PLAIN')\
    .option('kafka.sasl.jaas.config',kafka_jaas_config)\
    .load()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2=df.withColumn('value',col('value').cast('string'))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema=StructType([
    StructField('channel',StringType()),
    StructField('type',StringType()),
    StructField("data",ArrayType(
        StructType([
            StructField('symbol',StringType()),
            StructField('side',StringType()),
            StructField('price',StringType()),
            StructField('qty',FloatType()),
            StructField('ord_type',StringType()),
            StructField('trade_id',StringType()),
            StructField('timestamp',StringType())
        ])

    ))
    
        
])
    


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df3=df2.withColumn('value',from_json('value',schema=schema))
df4=df3.select(
    col('value.channel').alias('channel'),
    col('value.type').alias('type'),
    explode(col('value.data')).alias('trades')
)
df5=df4.select(
    col('channel'),
    col('type'),
    col('trades.symbol'),
    col('trades.side'),
    col('trades.price'),
    col('trades.qty'),
    col('trades.ord_type'),
    col('trades.trade_id'),
    col('trades.timestamp')
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df4=df3.select(
    col('value.channel'),
    col('value.type'),
    col('value.data.symbol'),
    col('value.data.side'),
    col('value.data.price'),
    col('value.data.qty'),
    col('value.data.ord_type'),
    col('value.data.trade_id'),
    col('value.data.timestamp')
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df5=df5.withColumn('price',col('price').cast('float'))
df5=df5.withColumn('timestamp',col('timestamp').cast(TimestampType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query=(df5.writeStream.format('delta')\
            .outputMode('append')\
            .trigger(processingTime="10 seconds")\
            .option('checkpointLocation','Files/cp_checkpointLocation/cp_trades_transformation')\
            .toTable('Trades')
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # tHE  eND .   .   .
