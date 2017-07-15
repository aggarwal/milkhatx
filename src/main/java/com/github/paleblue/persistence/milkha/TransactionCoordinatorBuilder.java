package com.github.paleblue.persistence.milkha;


import static com.github.paleblue.persistence.milkha.util.Preconditions.checkNotNull;

import java.util.concurrent.ExecutorService;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

public class TransactionCoordinatorBuilder {

    private static final long DEFAULT_MAX_TIME_TO_COMMIT_OR_ROLLBACK_MILLIS = 5000L;
    private static final long DEFAULT_WAIT_PERIOD_BEFORE_SWEEPER_UNLOCK_MILLIS = 10000L;
    private static final long DEFAULT_WAIT_PERIOD_BEFORE_SWEEPER_DELETE_MILLIS = 20000L;

    private AmazonDynamoDB ddbClient;
    private ExecutorService executorService;
    private long maxTimeToCommitOrRollbackMillis;
    private long waitPeriodBeforeSweeperUnlockMillis;
    private long waitPeriodBeforeSweeperDeleteMillis;

    public TransactionCoordinatorBuilder(AmazonDynamoDB ddbClient, ExecutorService executorService) {
        this.ddbClient = checkNotNull(ddbClient);
        this.executorService = checkNotNull(executorService);
        this.maxTimeToCommitOrRollbackMillis = DEFAULT_MAX_TIME_TO_COMMIT_OR_ROLLBACK_MILLIS;
        this.waitPeriodBeforeSweeperUnlockMillis = DEFAULT_WAIT_PERIOD_BEFORE_SWEEPER_UNLOCK_MILLIS;
        this.waitPeriodBeforeSweeperDeleteMillis = DEFAULT_WAIT_PERIOD_BEFORE_SWEEPER_DELETE_MILLIS;
    }

    public TransactionCoordinatorBuilder withAmazonDynamoDBClient(final AmazonDynamoDB newDDBClient) {
        this.ddbClient = newDDBClient;
        return this;
    }

    public TransactionCoordinatorBuilder withMaxTimeToCommitOrRollbackMillis(final long newMaxTimeToCommitOrRollbackMillis) {
        this.maxTimeToCommitOrRollbackMillis = newMaxTimeToCommitOrRollbackMillis;
        return this;
    }

    public TransactionCoordinatorBuilder withWaitPeriodBeforeSweeperUnlockMillis(final long newWaitPeriodBeforeSweeperUnlockMillis) {
        this.waitPeriodBeforeSweeperUnlockMillis = newWaitPeriodBeforeSweeperUnlockMillis;
        return this;
    }

    public TransactionCoordinatorBuilder withWaitPeriodBeforeSweeperDeleteMillis(final long newWaitPeriodBeforeSweeperDeleteMillis) {
        this.waitPeriodBeforeSweeperDeleteMillis = newWaitPeriodBeforeSweeperDeleteMillis;
        return this;
    }

    public TransactionCoordinator build() {
        return new TransactionCoordinator(this.ddbClient,
                this.executorService,
                this.maxTimeToCommitOrRollbackMillis,
                this.waitPeriodBeforeSweeperUnlockMillis,
                this.waitPeriodBeforeSweeperDeleteMillis);
    }
}
