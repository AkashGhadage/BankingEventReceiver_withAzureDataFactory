What things did you considered of during the implementation?  
**What I understood:**  

**Problem Statement:** The message receiver utilizes Azure Service Bus with the Peek Lock method to read messages at least once. Your task is to create a production-quality event receiver that processes Credit and Debit notifications and updates the total balance in the BankAccounts table. You can add new tables to the database if necessary. The focus should be on production quality, encompassing quality levels, resiliency, and data integrity.

**Requirements for the MessageProcessor:**
- The MessageProcessor operates in multiple containers simultaneously.
- If IEventReceiver.Peek returns null, it indicates that there are no messages in the queue; therefore, the process should wait for 10 seconds.
- Messages that have been abandoned more than three times should automatically go to the Dead Letter Queue (this does not require additional coding).
- For Credit messages: Add the amount to the existing balance.
- For Debit messages: Deduct the amount from the existing balance.
-Transient failures should be retried exponentially after 5, 25, and 125 seconds.
-Non-transient failures should be moved to the Dead Letter Queue immediately. For example, message types other than Credit or Debit should be sent to the Dead Letter Queue during the first processing attempt.

**What I implemented:** I implemented the solution using Azure Data Factory (ADF) and Python, while the original repository was in C#. This necessitated using new services, which is somewhat outside the typical responsibilities of an Azure Data Engineer.

**I orchestrated an end-to-end workflow with ADF,** assuming that Azure Service Bus was configured with the Peek Lock method. To trigger the ADF pipeline upon message arrival, I used Logic Apps. Within the ADF pipeline, I incorporated a Databricks notebook activity to execute a Databricks notebook written in Python. This notebook reads the messages and processes them according to the specified requirements. Finally, I assumed that the necessary tables are available in Unity Catalog.


**Assumptions Made During Implementation:**  

**- Azure Service Bus Configuration:** It is assumed that the Azure Service Bus is properly configured to use the Peek Lock method for message retrieval, ensuring at-least-once delivery.

**- Table Availability:** It is assumed that the BankAccounts table is available in Unity Catalog.

**- Logic Apps for Triggering:** It is assumed that Logic Apps can effectively trigger the ADF pipeline on message arrival, enabling real-time processing of Credit and Debit notifications.

**High level implementation design:**
![HLD of Banking Event Receiver with Azure Data Factory](https://github.com/user-attachments/assets/4c8dcc49-f27c-4b89-8549-566260ed0144)


Anything was unclear?

