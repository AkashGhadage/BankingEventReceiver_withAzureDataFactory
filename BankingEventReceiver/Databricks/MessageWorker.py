import time
import json
import logging
import asyncio
from azure.servicebus import ServiceBusClient, ServiceBusReceiveMode
from pyspark.sql import SparkSession
from delta.tables import *


# Set up logging
logging.basicConfig(level=logging.INFO)

# Azure Service Bus connection details
SERVICE_BUS_CONNECTION_STRING = '<Service-Bus-Connection-String>'
QUEUE_NAME = '<Queue-Name>'  #TOPIC name

# Initialize SC
spark = SparkSession.builder \
    .appName("BankAccountProcessor") \
    .getOrCreate()

class MessageProcessor:
    def __init__(self):
        self.client = ServiceBusClient.from_connection_string(SERVICE_BUS_CONNECTION_STRING)

    async def process_message(self, message):
        try:
            logging.info(f"Received message: {message}")
            message_data = json.loads(str(message))
            message_type = message_data.get('messageType')
            bank_account_id = message_data.get('bankAccountId')
            amount = message_data.get('amount')
 
            # Requirements:
            # Credit messages: Add amount to the existing balance
            # Debit messages: Deduct amount from the existing balance
            # ACTION :  Set flag based and tramsaction type and do processessing accordingly  
            if message_type == "Credit":
                await self.update_balance(bank_account_id, amount, is_credit=True)
            elif message_type == "Debit":
                await self.update_balance(bank_account_id, amount, is_credit=False)
            else:
                logging.error(f"Unknown message type: {message_type}. Moving to deadletter.")
                await self.move_to_deadletter(message)

        except Exception as e:
            logging.error(f"Error processing message: {e}. Moving to deadletter.")
            await self.move_to_deadletter(message)

    async def update_balance(self, bank_account_id, amount, is_credit):
        #Requirements:
        # Transient failures needs to be exponentially retried by 5, 25 and 125 seconds.
        # Non-transient failures should be moved to deadletter on the spot
        
        retry_attempts = [5, 25, 125]  
        for attempt in range(len(retry_attempts)):
            try:
                # Load the Delta table from Unity Catalog database
                bank_accounts_df = spark.read.table("unity_catalog.database_schema.BankAccounts")

                # select specific bank account
                account_row = bank_accounts_df.filter(bank_accounts_df.id == bank_account_id).first()

                if account_row is None:
                    logging.warning(f"Bank account ID {bank_account_id} not found.")
                    return
                
                # new balance calculation based on credit debit
                new_balance = account_row.balance + amount if is_credit else account_row.balance - amount

                # Update the balance in the Delta table
                delta_table = DeltaTable.forPath(spark, "path_to_your_delta_table")  # Provide the correct path
                delta_table.update(
                    condition=f"id = '{bank_account_id}'",
                    set={"balance": new_balance}
                )

                logging.info(f"Updated balance for account {bank_account_id}: {new_balance}")
                break  

            except Exception as e:
                logging.error(f"Error updating balance: {e}. Retrying in {retry_attempts[attempt]} seconds.")
                # Wait before retryign
                time.sleep(retry_attempts[attempt])  
                if attempt == len(retry_attempts) - 1:
                    logging.error(f"Moving message to deadletter after multiple failures.")
                    await self.move_to_deadletter(message)

    async def move_to_deadletter(self, message):
        logging.info(f"Moving message to deadletter: {message}")


#   Requirement 1 : If IEventReceiver.Peek returns null, it means there are no messages in the queue, so await 10 seconds  
    async def run(self):
        while True:
            async with self.client.get_queue_receiver(queue_name=QUEUE_NAME, receive_mode=ServiceBusReceiveMode.PEEK_LOCK) as receiver:
                # Peek for messages
                peeked_messages = await receiver.peek_messages(max_message_count=1)
                
                if not peeked_messages:
                    logging.info("No messages found in the queue. Waiting for 10 seconds.")
                    await asyncio.sleep(10) 
                    continue 

                
                messages = await receiver.receive_messages(max_message_count=1, max_wait_time=10)
                
                for message in messages:
                    await self.process_message(message)
                    await receiver.complete_message(message) 

if __name__ == "__main__":
    processor = MessageProcessor()
    asyncio.run(processor.run())
