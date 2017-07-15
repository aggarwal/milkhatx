package com.github.paleblue.persistence.milkha;


import static com.github.paleblue.persistence.milkha.util.Preconditions.checkNotNull;

import java.util.concurrent.ScheduledExecutorService;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

public class TransactionSweeperBuilder {
    private static final int DEFAULT_TRANSACTION_LOG_TABLE_SCAN_PAGE_SIZE = 1000;
    private static final long DEFAULT_TRANSACTION_SWEEPER_DELAY_SECONDS = 30L;

    private final AmazonDynamoDB ddbClient;
    private final ScheduledExecutorService scheduledExecutorService;
    private int transactionLogTableScanPageSize;
    private long transactionSweeperDelaySeconds;

    public TransactionSweeperBuilder(AmazonDynamoDB ddbClient, ScheduledExecutorService scheduledExecutorService) {
        this.ddbClient = checkNotNull(ddbClient);
        this.scheduledExecutorService = checkNotNull(scheduledExecutorService);
        this.transactionLogTableScanPageSize = DEFAULT_TRANSACTION_LOG_TABLE_SCAN_PAGE_SIZE;
        this.transactionSweeperDelaySeconds = DEFAULT_TRANSACTION_SWEEPER_DELAY_SECONDS;
    }

    public TransactionSweeperBuilder withTransactionLogTableScanPageSize(final int newTransactionLogTableScanPageSize) {
        this.transactionLogTableScanPageSize = newTransactionLogTableScanPageSize;
        return this;
    }

    public TransactionSweeperBuilder withTransactionSweeperDelaySeconds(final long newTransactionSweeperDelaySeconds) {
        this.transactionSweeperDelaySeconds = newTransactionSweeperDelaySeconds;
        return this;
    }

    public TransactionSweeper build() {
        return new TransactionSweeper(ddbClient, scheduledExecutorService, transactionLogTableScanPageSize, transactionSweeperDelaySeconds);
    }
}
