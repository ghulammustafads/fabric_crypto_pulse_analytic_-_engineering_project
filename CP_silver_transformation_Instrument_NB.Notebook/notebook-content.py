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

kafka_jaas_config = (
    'org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="{parser['kafka']['KAFKA_API_KEY']}" '
    f'password="{parser['kafka']['KAFKA_API_SECRET']}";'
)

df=spark.readStream.format('kafka')\
            .option('kafka.bootstrap.servers',parser['kafka']['KAFKA_BOOTSTRAP_SERVERS'])\
            .option('subscribe',topic['kafka_topics']['KAFKA_TOPIC_TICKETS'])\
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

schema = StructType([
    StructField('channel', StringType(), True),
    StructField('type', StringType(), True),
    StructField('data', StructType([
        StructField('pairs', ArrayType(StructType([
            StructField('symbol', StringType(), True),
            StructField('base', StringType(), True),
            StructField('quote', StringType(), True),
            StructField('status', StringType(), True),
            StructField('qty_precision', IntegerType(), True),         # Changed from String
            StructField('qty_increment', FloatType(), True),         # Handle scientific notation like 1e-08
            StructField('price_precision', IntegerType(), True),       # Changed from String
            StructField('cost_precision', IntegerType(), True),        # Newly added field found in your JSON
            StructField('marginable', BooleanType(), True),            # Changed from String (handles false)
            StructField('has_index', BooleanType(), True),             # Changed from String (handles false)
            StructField('ws_display_price_precision', IntegerType(), True),
            StructField('cost_min', FloatType(), True),               # Changed from String (handles 0.5)
            StructField('tick_size', FloatType(), True),              # Changed from String (handles 0.01)
            StructField('price_increment', FloatType(), True),        # Changed from String (handles 0.01)
            StructField('qty_min', FloatType(), True)                 # Changed from String (handles 0.002)
        ])), True)
    ]), True)
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
    explode(col('value.data.pairs')).alias('pairs')
)
df4=df3.select(
    'channel',
    'type',
   
    col('pairs.base').alias('base'),
    col('pairs.quote').alias('quote'),
    col('pairs.status').alias('status'),
    col('pairs.qty_increment').alias('qty_increment'),
    col('pairs.qty_precision').alias('qty_precision'),
    col('pairs.price_precision').alias('price_precision'),
    col('pairs.cost_precision').alias('cost_precision'),
    col('pairs.marginable').alias('marginable'),
    col('pairs.has_index').alias('has_index'),
    col('pairs.ws_display_price_precision').alias('ws_display_price_precision'),
    col('pairs.cost_min').alias('cost_min'),
    col('pairs.tick_size').alias('tick_size'),
    col('pairs.price_increment').alias('price_increment'),
    col('pairs.qty_min').alias('qty_min')
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query=(df4.writeStream.format('delta') \
    .outputMode('append') \
    .trigger(processingTime='10 seconds') \
    .option('checkpointLocation', 'Files/cp_checkpointLocation/cp_instruments_transformation') \
    .toTable('Instruments')
)

query.awaitTermination()
logger.info('Ticker Notebook Ran Suceesfully and transformation the data ')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # tHE ENd . . . ! 
