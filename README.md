# Example ML Flow

## Objective
This is an example workload leveraging a tool that simulates credit card transaction data, including fraudulent transactions, which will inform a machine learning model on fraud detection. We'll then generate these transactions with a dbworkload utility and CDC them into a platform where AI can be used to identify potential issues in near real-time. This may be an overly simplified example, but it will allow us to demo an end-to-end ML flow that we can use as a template for real machine learning applications.

Some of the challenges related to feeding data into analytical systems is the time variance between when a transaction completes and when it is received for processing.  Also, combining the bitemporal nature of one entity with other data at different transactional boundaries introduces additional complexity.  For example, how do we know the state of a customer account at the exact moment a transaction completed?  Many data models may use slowly changing deminsions and "good until" timestamps to answer this question, which becomes burdemsome when you move from batch to streaming.  Luckily, cockroachdb provides features in their CDC feeds that allow us to identify when a group of unrelated transactions are guaranteed to be consistent with each other.

## Process Flow
The process flow is very simple
1) We'll generate some dummy credit card transactions
2) These will get loaded into a number of database tables
3) Completed transactions are passed through change data capture
4) The results of the cdc pipeline are sent to a storage bucket
5) Files from the storage bucket are feed to an analytical platform
6) The data is then resolved to match on an "as of" timestamp
7) Which can then be transformed and used as input into an ML application

<img src="https://github.com/user-attachments/assets/fae079c7-6bda-4739-88cd-a7e5651cf0b4" width="60%"/>

## Test the Flow
You can download the repository and run the workload locally to test different scenarios and configurations that are appropriate for your own ML flow.  More information on environment setup and the steps required to run the workload can be found on our [wiki](https://github.com/roachlong/example-ml-flow/wiki) pages.
