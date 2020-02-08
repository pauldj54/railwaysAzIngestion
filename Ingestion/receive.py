# receive.py

import asyncio
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore


async def on_event(partition_context, event):
    # print the event data
    print("Received the event: \"{}\" from the partition with ID: \"{}\"".format(event.body_as_str(encoding='UTF-8'), partition_context.partition_id))

    # update the checkpoint so that the program doesn't read the events
    # that it has already read when you run it next time
    await partition_context.update_checkpoint(event)

async def main():
    # create an Azure blob checkpoint store to store the checkpoints
    checkpoint_store = BlobCheckpointStore.from_connection_string("DefaultEndpointsProtocol=https;AccountName=omnetricprotomsn22012020;AccountKey=Nu685/HadchwA6fJ+2dZxPCCegoMf8Ni2HE5KoHK10RHr+iF/V/PplbvAY99fYCJYah6XqJwUBNGSwBxU0Pfpw==;EndpointSuffix=core.windows.net", "checkpointeventhubrailwayuk")

    # create a consumer client for the event hub
    client = EventHubConsumerClient.from_connection_string("Endpoint=sb://omnph20202101.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=ty62t/AUfWGY958yFUjHFAuGj5yHK1EI4xFXCJWT+eU=", consumer_group="$Default", eventhub_name="railwaysukopen", checkpoint_store=checkpoint_store)
    async with client:
        # call the receive method
        await client.receive(on_event=on_event)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # run the main method
    loop.run_until_complete(main())