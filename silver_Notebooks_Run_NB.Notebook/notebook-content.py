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

# # This Notebook Runs all Notebook of Silver Layer 

# CELL ********************

import logging 
# Creating The logger
logger = logging.getLogger('Silver_logger')
logger.setLevel('DEBUG')

# Creating The Handler for the loggers 
stream_handler=logging.StreamHandler()
file_handler=logging.handlers.RotatingFileHandler('/lakehouse/default/Files/crypto_silver_log/Silver_log',
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


from notebookutils import notebook

DAG = {
    "activities": [
        {
            "name": "Trades",
            "path": "CP_silver_Transformation_Trades_NB"
        },
        {
            "name": "OHLC",
            "path": "CP_silver_Transformation_ohlc_NB"
        }

        
    ],
    "timeoutInSeconds": 43200,
    "concurrency": 2
}

notebook.validateDAG(DAG)

results = notebook.runMultiple(DAG)

for name, result in results.items():
    if result["exception"]:
        print(f"{name}: FAILED")
        print(result["exception"])
    else:
        print(f"{name}: SUCCESS")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # tHE ENd
