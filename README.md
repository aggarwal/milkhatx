# MilkhaTx

## Limitations:
* Does not work with DynamoDBMapper. Users need to build their own marshalling. Library provides abstract marshaller class.
* Low-level data model is constrained by the immutability assumption.
* No support for lock stealing. Transactions have to wait for previous transactions to complete or be swept.

## Advantages:
* Supports LSIs, GSIs, Scans and Queries.
* Consumes less DynamoDB resources.
* Transactions are low-latency; 10s of milliseconds.

## Usage:
1. Create the TransactionsLog table with hash key "transactionId" and no range key.
2. Create your DAOs using the HashOnlyMapper or HashAndRangeMapper classes.
3. Instantiate a TransactionCoordinator object using TransactionCoordinatorBuilder.
4. Instantiate a TransactionSweeper object using TransactionSweeperBuilder. Kick-off the sweeper using the schedule() method and keep it running in the background.
5. Use the TransactionCoordinator object's public interface to do useful things.
