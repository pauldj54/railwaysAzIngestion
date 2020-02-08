# receive.py

import asyncio
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
import json

# Load Config file
with open(r"Ingestion\settings.json") as f:
  config = json.load(f)
print(config)

async def on_event(partition_context, event):
    # print the event data
    print("Received the event: \"{}\" from the partition with ID: \"{}\"".format(event.body_as_str(encoding='UTF-8'), partition_context.partition_id))

    # update the checkpoint so that the program doesn't read the events
    # that it has already read when you run it next time
    await partition_context.update_checkpoint(event)

async def main():
    # create an Azure blob checkpoint store to store the checkpoints
    checkpoint_store = BlobCheckpointStore.from_connection_string(config["storageAccountConnStr"], config["storageAccountName"])

    # create a consumer client for the event hub
    client = EventHubConsumerClient.from_connection_string(conn_str=config["eventHubConnStr"], consumer_group=config["consumerGroup"], eventhub_name=config["eventHubName"], checkpoint_store=checkpoint_store)
    async with client:
        # call the receive method
        await client.receive(on_event=on_event)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # run the main method
    loop.run_until_complete(main())