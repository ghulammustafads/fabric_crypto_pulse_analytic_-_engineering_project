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

# # Books Transformation Silver Notebook

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
import configparser
import logging 
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

# Reading the stream with required configraitons

kafka_jaas_config = (
    'org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="{parser['kafka']['KAFKA_API_KEY']}" '
    f'password="{parser['kafka']['KAFKA_API_SECRET']}";'
)

df=spark.readStream.format('kafka')\
            .option('kafka.bootstrap.servers',parser['kafka']['KAFKA_BOOTSTRAP_SERVERS'])\
            .option('subscribe',topic['kafka_topics']['KAFKA_TOPIC_BOOK'])\
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

# converting the value column from bytes to string 
df2=df.withColumn('value',col('value').cast('string')).select('value')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Creating the schema for the books data
schema = StructType([
    StructField('channel', StringType()),
    StructField('type', StringType()),

    StructField('data', ArrayType(StructType([
        StructField('symbol', StringType()),

        StructField('bids', ArrayType(StructType([
            StructField('price', StringType()),
            StructField('qty', StringType())
        ]))),

        StructField('asks', ArrayType(StructType([
            StructField('price', StringType()),
            StructField('qty', StringType())
        ]))),

        StructField('checksum', StringType()),
        StructField('timestamp', StringType())
    ])))
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Converting the value column into struct type 
df3=df2.withColumn('value',from_json(col('value'),schema=schema))

# Extracting column and  exploding data key from struct type
df4=df3.select(
    col('value.channel').alias('channel'),
    col('value.type').alias('type'),
    explode(col('value.data')).alias('data'),
)

#  Extracting symbole , timestamp bids , asks from exploded data key
df5=df4.withColumn('symbol',col('data.symbol'))\
        .withColumn('checksum',col('data.checksum'))\
        .withColumn('timestamp',col('data.timestamp'))\
        .withColumn('bids',explode('data.bids'))\
        .withColumn('asks',explode('data.asks'))

# selecting The final columns 
df6=df5.select(
    'channel',
    'type',
    'symbol',
    'checksum',
    'timestamp',
    col('bids.price').alias('bid_price'),
    col('bids.qty').alias('bid_qty'),
    col('asks.price').alias('asks_price'),
    col('asks.qty').alias('asks_qty')
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# converting the data types of columns 

df7=df6.withColumns(
    {
        'checksum':col('checksum').cast('float'),
        'timestamp':col('timestamp').cast('timestamp'),
        'bid_price':col('bid_price').cast('float'),
        'bid_qty':col('bid_qty').cast('float'),
        'asks_price':col('asks_price').cast('float'),
        'asks_qty':col('asks_qty').cast('float')
    }
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Writing The stream into the lakhosue 

query=(df7.writeStream.format('delta')\
                .outputMode('append')\
                .trigger(processingTime='10 seconds')\
                .option('checkpointLocation','Files/cp_checkpointLocation/cp_books_transformation')\
                .toTable('Books')
)

query.awaitTermination()
logger.info('Notebook Ran  and Loaded data Successfully')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # tHE  eND  .  .  . !
