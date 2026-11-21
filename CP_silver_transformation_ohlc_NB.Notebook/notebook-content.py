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
# META     }
# META   }
# META }

# MARKDOWN ********************

# # CP Data Transformation Silver Layer - OHLC

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
import configparser
from IPython.display import JSON
import logging 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

logger = logging.getLogger('cp_silver_transformation_log')
logger.setLevel('DEBUG')

stream_handler=logging.StreamHandler()
file_handler=logging.handlers.RotatingFileHandler('/lakehouse/default/Files/cp_silver_transformation_logs/cp_silver_ohlc',
                maxBytes=1024000 , backupCount=5)

stream_handler.setLevel('INFO')
file_handler.setLevel('DEBUG')

stream_handler.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

logger.addHandler(stream_handler)
logger.addHandler(file_handler)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

parser=configparser.ConfigParser()
parser.read('/lakehouse/default/Files/silver_kafka_credentials/kafka_credentials.ini')

topic=configparser.ConfigParser()
topic.read('/lakehouse/default/Files/silver_kafka_credentials/kafka_topics.ini')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

kafka_jaas_config = (
    'org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="{parser['kafka']['KAFKA_API_KEY']}" '
    f'password="{parser['kafka']['KAFKA_API_SECRET']}";'
)

df=spark.readStream.format('kafka')\
            .option('kafka.bootstrap.servers',parser['kafka']['KAFKA_BOOTSTRAP_SERVERS'])\
            .option('subscribe',topic['kafka_topics']['KAFKA_TOPIC_OHLC'])\
            .option('startingoffsets','earliest')\
            .option('kafka.security.protocol','SASL_SSL')\
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
df3=df2.select('value')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema=StructType([
    StructField('channel',StringType()),
    StructField('type',StringType()),
    StructField('data',ArrayType(StructType([
        StructField('symbol',StringType()),
        StructField('open',StringType()),
        StructField('high',StringType()),
        StructField('low',StringType()),
        StructField('close',StringType()),
        StructField('trades',StringType()),
        StructField('volume',StringType()),
        StructField('vwap',StringType()),
        StructField('interval_begin',StringType()),
        StructField('interval',StringType()),
        StructField('timestamp',StringType())
        ])
    ))
]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df4=df3.withColumn('value',from_json(col('value'),schema=schema))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df5=df4.select(
    col('value.channel').alias('channel'),
    col('value.type').alias('type'),
    explode('value.data').alias('ohlc')
)

df6=df5.select(
    'channel',
    'type',
    col('ohlc.symbol'),
    col('ohlc.open'),
    col('ohlc.high'),
    col('ohlc.low'),
    col('ohlc.close'),
    col('ohlc.trades'),
    col('ohlc.vwap'),
    col('ohlc.interval_begin'),

    col('ohlc.interval'),
    col('ohlc.timestamp'),
    col('ohlc.volume')
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df7=df6.withColumns(
    {
        
    'high' : col('high').cast('float'),
    'low':col('low').cast('float'),
    'open':col('open').cast('float'),
    'close':col('close').cast('float'),
    'timestamp':col('timestamp').cast('timestamp'),
    'interval_begin':col('interval_begin').cast('timestamp'),
    'trades':col('trades').cast('float'),
    'volume':col('volume').cast('float')
}
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query = (
    df7.writeStream
    .format('delta')
    .outputMode('append')
    .trigger(processingTime="10 seconds")
    .option('checkpointLocation', 'Files/cp_checkpointLocation/cp_ohlc_transformation')
    .toTable('ohlc_')
)
logger.info('Notebook Was executed and data was transformaed and loaded into the lakhosue')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # tHE   eND  .   .  .  !
