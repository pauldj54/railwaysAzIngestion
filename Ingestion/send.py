import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

import json

# Load Config file
with open("Ingestion\settings.json") as f:
  config = json.load(f)
print(config)

async def run(event):
    # create a producer client to send messages to the event hub
    # specify connection string to your event hubs namespace and
 	    # the event hub name
    producer = EventHubProducerClient.from_connection_string(conn_str="Endpoint=sb://omnph20202101.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=ty62t/AUfWGY958yFUjHFAuGj5yHK1EI4xFXCJWT+eU=", eventhub_name="railwaysukopen")
    producer = EventHubProducerClient.from_connection_string(conn_str=config["eventHubConnStr"], eventhub_name=config["eventHubName"])
        
    async with producer:
        # create a batch
        event_data_batch = await producer.create_batch()

        # add events to the batch
        event_data_batch.add(EventData(event))
 
        # send the batch of events to the event hub
        await producer.send_batch(event_data_batch)

#loop = asyncio.get_event_loop()
#loop.run_until_complete(run())