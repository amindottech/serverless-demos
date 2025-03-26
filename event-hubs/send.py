import asyncio

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

import os


# get from environment variables

EVENT_HUB_CONNECTION_STR = os.getenv("EVENT_HUB_CONNECTION_STR")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")
if not EVENT_HUB_CONNECTION_STR or not EVENT_HUB_NAME:
    raise ValueError("Please set the EVENT_HUB_CONNECTION_STR and EVENT_HUB_NAME environment variables.")   


async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
    )
    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()

        # Add events to the batch.
        event_data_batch.add(EventData("CST8917-1 "))
        event_data_batch.add(EventData("CST8917-2 "))
        event_data_batch.add(EventData("CST8917-3 "))

        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)

asyncio.run(run())