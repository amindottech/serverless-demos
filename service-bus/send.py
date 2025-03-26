import asyncio
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusMessage
import sys
import os

#get from environment variables
NAMESPACE_CONNECTION_STR = os.environ["SERVICE_BUS_CONNECTION_STR"]
QUEUE_NAME = os.environ["QUEUE_NAME"]
if not NAMESPACE_CONNECTION_STR or not QUEUE_NAME:
    raise ValueError("Please set the environment variables SERVICE_BUS_CONNECTION_STR and QUEUE_NAME")


async def send_single_message(sender):
    # Create a Service Bus message and send it to the queue
    message = ServiceBusMessage("Single Message")
    await sender.send_messages(message)
    print("Sent a single message")

async def send_a_list_of_messages(sender):
    # Create a list of messages and send it to the queue
    messages = [ServiceBusMessage("Message in list") for _ in range(5)]
    await sender.send_messages(messages)
    print("Sent a list of 5 messages")

async def send_batch_message(sender):
    # Create a batch of messages
    async with sender:
        batch_message = await sender.create_message_batch()
        for _ in range(10):
            try:
                # Add a message to the batch
                batch_message.add_message(ServiceBusMessage("Message inside a ServiceBusMessageBatch"))
            except ValueError:
                # ServiceBusMessageBatch object reaches max_size.
                # New ServiceBusMessageBatch object can be created here to send more data.
                break
        # Send the batch of messages to the queue
        await sender.send_messages(batch_message)
    print("Sent a batch of 10 messages")

async def send_message_from_user(sender, user_message):
    # Create a Service Bus message from user input and send it to the queue
    message = ServiceBusMessage(user_message)
    await sender.send_messages(message)
    print(f"Sent message: {user_message}")

async def run(user_message):
    # create a Service Bus client using the connection string
    async with ServiceBusClient.from_connection_string(
        conn_str=NAMESPACE_CONNECTION_STR,
        logging_enable=True) as servicebus_client:
        # Get a Queue Sender object to send messages to the queue
        sender = servicebus_client.get_queue_sender(queue_name=QUEUE_NAME)
        async with sender:
            if user_message is not None:
                # Send messages from user input
                await send_message_from_user(sender, user_message)
            else:
                # Send messages in different formats
                print("Sending messages...")
                # Send a single message
                await send_single_message(sender)
                # Send a list of messages
                await send_a_list_of_messages(sender)
                # Send a batch of messages
                await send_batch_message(sender)

if __name__ == "__main__":
    # get parameters from command line
    user_message = sys.argv[1] if len(sys.argv) > 1 else None
    
    # run the send function
    asyncio.run(run(user_message))
