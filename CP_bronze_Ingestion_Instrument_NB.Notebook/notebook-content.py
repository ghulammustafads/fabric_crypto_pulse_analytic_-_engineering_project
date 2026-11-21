# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b7da6611-4be2-4d80-990a-2002d950a0e1",
# META       "default_lakehouse_name": "bronze_kraken_trades",
# META       "default_lakehouse_workspace_id": "527fe858-1cca-4c4f-9ab8-d09f43eb5b5f",
# META       "known_lakehouses": [
# META         {
# META           "id": "b7da6611-4be2-4d80-990a-2002d950a0e1"
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

# ## Crypto Instrument data Ingestion Notebook Using Websocket

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
import websocket
import json
import datetime
from  confluent_kafka import Producer
import configparser
import logging 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Creating The Logger
logger=logging.getLogger('Instructment_logger')
logger.setLevel(logging.DEBUG)

# Creating a RotatingFile Handler with required connfigrations 
rotating_file_handler=logging.handlers.RotatingFileHandler('/lakehouse/default/Files/bronze_Ingestion_NBs_log/Instrucment_Ingestion__NB_log.lg',
                        maxBytes=1024000,
                        backupCount=5)
rotating_file_handler.setLevel(logging.WARNING)
rotating_file_handler.setFormatter(logging.Formatter('%(levelname)s  |  %(message)s  |  %(asctime)s'))
logger.addHandler(rotating_file_handler)


# Creating  a Stream Handler and Adding into the logger
stream_handler=logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(logging.Formatter('%(levelname)s  -  %(message)s'))
logger.addHandler(stream_handler)

logger.info('Created custom Logger with Required configuraitons with Tow handers 1.RotatingFileHandler 2.StreamHandler')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Read configuration file
config = configparser.ConfigParser()
config.read("/lakehouse/default/Files/bronze_crypto_kafka_credentials/kafka_credentials.ini")

# Load Kafka credentials
bootrap_server = config.get("kafka", "KAFKA_BOOTSTRAP_SERVERS")
security_protocole = config.get("kafka", "KAFKA_SECURITY_PROTOCOL")
mechanism = config.get("kafka", "KAFKA_SASL_MECHANISM")
user_name = config.get("kafka", "KAFKA_API_KEY")
password = config.get("kafka", "KAFKA_API_SECRET")

print("Kafka credentials loaded successfully!")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

conf={
    'bootstrap.servers':bootrap_server,
    'security.protocol': security_protocole,
    'sasl.mechanism':mechanism,
    'sasl.username':user_name,
    'sasl.password':password
}
producer=Producer(conf)
logger.info('Creating Producers With Required Configuration')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def on_open(ws) : 
    ws.send(json.dumps(
        {
    "method": "subscribe",
    "params": {
        "channel": "instrument"
    },
    "req_id": 79
}
))
logger.info('Create open function for logger')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Defined the filename variable
filename = f"/lakehouse/default/Files/bronze_crypto_instruments/{datetime.datetime.now()}_instrument.jsonl"

def on_message(ws, message):
    data = json.loads(message)
    if data.get('channel') != 'instrument':
        return
    #for intstrument in data['data']['assets']:
        # raw_data={
          #  'channel':data['channel'],
         #   'type':data['type'],
        #    'id':intstrument['id'],
       #     'status':intstrument['status'],
      #      'precision':intstrument['precision'],
     #       'precision_display':intstrument['precision_display'],
    #        'borrowable':intstrument['borrowable'],
   #         'collateral_value':intstrument['collateral_value'],
  #          'class':intstrument['class'],
 #           'margin_rate':intstrument['margin_rate']
#
        #}
       
    
    # 2. Used the correct variable name (filename) and added a newline (\n)
    producer.produce(topic='crypto.market.tickets',value=json.dumps(data))

    with open(file_name,'a') as f:
    
        f.write(json.dumps(data))
    
logger.info('Create on_message function which gets messages from webscket,saves in a file in lakhouse , pushes on kafka topic as well')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def on_error(ws,error):
    print('Error',error)
logger.info('Created on_error function')
logger.info('Create on errro funciton which captures error if happen')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

url='wss://ws.kraken.com/v2'
ws=websocket.WebSocketApp(
    url=url,
    on_open=on_open,
    on_message=on_message,
    on_error=on_error
)
ws.run_forever()
logger.info('Creating the websocket object with required configurations and runing it.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 

# MARKDOWN ********************

# # tHE eND .  .  .   !
