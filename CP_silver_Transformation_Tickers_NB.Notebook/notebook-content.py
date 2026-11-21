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
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Ticker RT Streaming Silver Layer (Transformation) 

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging 
import configparser
from IPython.display import JSON



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Creating The logger
logger = logging.getLogger('cp_silver_transformation_log')
logger.setLevel('DEBUG')

# Creating The Handler for the loggers 
stream_handler=logging.StreamHandler()
file_handler=logging.handlers.RotatingFileHandler('/lakehouse/default/Files/cp_silver_transformation_logs/cp_silver_books',
                maxBytes=1024000 , backupCount=5)

# Setting The level for handlers
stream_handler.setLevel('INFO')
file_handler.setLevel('DEBUG')

# Setting the formates of handlers output 
stream_handler.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# adding handlers to created logger 
logger.addHandler(stream_handler)
logger.addHandler(file_handler)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Creating parser and reading kafka credentials 
parser=configparser.ConfigParser()
parser.read('/lakehouse/default/Files/silver_kafka_credentials/kafka_credentials.ini')

# Creating parser for topic to read topics from file
topic=configparser.ConfigParser()
topic.read('/lakehouse/default/Files/silver_kafka_credentials/kafka_topics.ini')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

topic['kafka_topics']['KAFKA_TOPIC_TICKETS']

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
            .option('subscribe',topic['kafka_topics']['KAFKA_TOPIC_TICKERS'])\
            .option('kafka.security.protocol','SASL_SSL')\
            .option('startingoffset','earliest')\
            .option('kafka.sasl.mechanism','PLAIN')\
            .option('kafka.sasl.jaas.config',kafka_jaas_config)\
            .load()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1=df.withColumn('value',col('value').cast('string')).select('value')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema=StructType([
    StructField('channel',StringType()),
    StructField('type',StringType()),
    StructField('data',ArrayType(
        StructType([
            StructField('symbol',StringType()),
            StructField('bid',FloatType()),
            StructField('bid_qty',FloatType()),
            StructField('ask',FloatType()),
            StructField('ask_qty',FloatType()),
            StructField('last',FloatType()),
            StructField('volum',FloatType()),
            StructField('vwap',FloatType()),
            StructField('low',FloatType()),
            StructField('high',FloatType()),
            StructField('change',FloatType()),
            StructField('change_pct',FloatType()),
            StructField('trades',IntegerType()),
            StructField('timestamp',TimestampType())


        ])
        
    ))
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2=df1.withColumn('value',from_json(col('value'),schema=schema))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df3=df2.select(
    col('value.channel').alias('channel'),
    col('value.type').alias('type'),
    explode('value.data').alias('ticker')
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df4=df3.select(
    'channel',
    'type',
    col('ticker.symbol').alias('symbol'),
    col('ticker.trades').alias('trades'),
    col('ticker.bid').alias('bid'),
    col('ticker.bid_qty').alias('bid_qty'),
    col('ticker.last').alias('last'),
    col('ticker.vwap').alias('vwap'),
    col('ticker.ask').alias('ask'),
    col('ticker.ask_qty').alias('ask_qty'),
    col('ticker.change').alias('change'),
    col('ticker.change_pct').alias('change_pct'),
    col('ticker.high').alias('high'),
    col('ticker.low').alias('low'),
    col('ticker.timestamp').alias('timestamp')
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query=(df4.writeStream.format('delta')\
            .outputMode('append')\
            .trigger(processingTime="10 seconds")\
            .option('checkpointLocation','Files/cp_checkpointLocation/cp_tickers_transformtion')\
            .toTable('Tickers')
)
query.awaitTermination()
logger.info('Transformaed and loaded Ticker data ')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # tHE ENd   .   .  . !
