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

# # Real time Crypto books data Ingestion Using websocket & Kafka 

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import  websocket
import configparser
from confluent_kafka import Producer
import datetime
import logging

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

logger=logging.getLogger('Book_NB_Logger')

logger.setLevel('DEBUG')

Stream_handler=logging.StreamHandler()
rotating_handler=logging.handlers.RotatingFileHandler(
                        '/lakehouse/default/Files/bronze_Ingestion_NBs_log/book_Ingestion_NB_log.lg',
                        maxBytes = 1024000,
                        backupCount=5
                        )
Stream_handler.setFormatter(logging.Formatter('%(levelname)s  |  %(name)s  |  %(message)s  '))
rotating_handler.setFormatter(logging.Formatter('%(levelname)s  |  %(name)s  |  %(message)s  |  %(asctime)s'))

Stream_handler.setLevel('WARNING')

logger.addHandler(rotating_handler)
logger.addHandler(Stream_handler)

logger.info('Created custome Logger With Required Configrations')

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

def on_open(ws):
    ws.send(json.dumps(
        
        {
        "method": "subscribe",
        "params": {
            "channel": "book",
            "symbol": [
            "BTC/USD",
            "ETH/USD",
            "SOL/USD",
            "XRP/USD",
            "ADA/USD",
            "DOGE/USD",
            "AVAX/USD",
            "LINK/USD",
            "DOT/USD",
            "ALGO/USD"
            ],
            "depth": 10,
            "snapshot": True
           
        }
        }

    ))
logger.info('Sent Required input data to websocket to start the connection')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# {'channel': 'book', 'type': 'snapshot', 'data': [{'symbol': 'ETH/USD', 'bids': [{'price': 2254.0, 'qty': 2.9233332}, {'price': 2253.96, 'qty': 0.76721448}, {'price': 2253.95, 'qty': 0.66217134}, {'price': 2253.94, 'qty': 2.21832945}, {'price': 2253.74, 'qty': 0.01035749}, {'price': 2253.73, 'qty': 1.08741774}, {'price': 2253.72, 'qty': 0.026}, {'price': 2253.68, 'qty': 3.98201859}, {'price': 2253.62, 'qty': 4.45459786}, {'price': 2253.61, 'qty': 8.87249791}], 'asks': [{'price': 2254.01, 'qty': 2.72443544}, {'price': 2254.05, 'qty': 10.6469975}, {'price': 2254.16, 'qty': 2.21812915}, {'price': 2254.18, 'qty': 0.2}, {'price': 2254.27, 'qty': 10.14944116}, {'price': 2254.29, 'qty': 0.55722806}, {'price': 2254.3, 'qty': 1.5}, {'price': 2254.34, 'qty': 0.36676068}, {'price': 2254.4, 'qty': 0.25560557}, {'price': 2254.42, 'qty': 6.90714519}], 'checksum': 1285742254, 'timestamp': '2026-10-31T18:17:05.878680Z'}]}


# CELL ********************


file_name=f"/lakehouse/default/Files/bronze_crypto_book/{datetime.datetime.now()}_book.jsonl"
def on_message(ws, message):
    data = json.loads(message)

    if data.get('channel') != 'book':
        return

    for books in data['data']:

    #    for bids, asks in zip(books['bids'], books['asks']):
#
    #        raw_data = {
    #            'channel': data['channel'],
    #            'type': data['type'],
    #            'symbol': books['symbol'],
#
    #            'bid_price': bids['price'],
  ##              'bid_qty': bids['qty'],
#
  #              'ask_price': asks['price'],
          #      'ask_qty': asks['qty'],
#
            #    'checksum': books['checksum'],
            #    'timestamp': books['timestamp']
            #}

            
            #producer.produce(topic='esehamlhpbj16gug62k2dy_eh',value=json.dumps(raw_data))
        producer.produce(topic='crypto.market.orderbook',value=json.dumps(raw_data))
        with open(file_name,'a') as f:
            f.write(json.dumps(data))
        print(data)
   

logger.info('Created on_message function which gets responses from websocket , sents it on both lakhouse and kafka topic')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def on_error(ws,error):
    print('Error Occured : ',error)
logger.info('Created Error Function To throw an approperiate error')

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # tHE  eND  .   .   .
