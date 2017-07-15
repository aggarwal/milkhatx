package com.github.paleblue.persistence.milkha;


import static com.github.paleblue.persistence.milkha.util.Preconditions.checkArgument;
import static com.github.paleblue.persistence.milkha.util.Preconditions.checkNotNull;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.github.paleblue.persistence.milkha.dto.TransactionLogItem;
import com.github.paleblue.persistence.milkha.mapper.TransactionLogItemMapper;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.Select;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class TransactionSweeper {

    private static final Log LOG = LogFactory.getLog(TransactionSweeper.class);

    private final TransactionLogItemMapper txLogItemMapper;
    private final ScheduledExecutorService scheduledExecutorService;
    private final AmazonDynamoDB ddbClient;
    private final int txLogTableScanPageSize;
    private final long txSweeperFixedDelaySeconds;
    private Map<String, TransactionSweeperTask> seenTransactions;

    TransactionSweeper(AmazonDynamoDB ddbClient, ScheduledExecutorService scheduledExecutorService, int txLogTableScanPageSize, long txSweeperFixedDelaySeconds) {
        this.scheduledExecutorService = checkNotNull(scheduledExecutorService);
        this.ddbClient = checkNotNull(ddbClient);
        checkArgument(txLogTableScanPageSize > 0, "txLogTableScanPageSize must be greater than zero.");
        checkArgument(txSweeperFixedDelaySeconds > 0, "txSweeperFixedDelaySeconds must be greater than zero.");
        this.txLogTableScanPageSize = txLogTableScanPageSize;
        this.txSweeperFixedDelaySeconds = txSweeperFixedDelaySeconds;
        this.txLogItemMapper = new TransactionLogItemMapper();
        seenTransactions = new ConcurrentHashMap<>();
    }

    public void schedule() {
        scheduledExecutorService.scheduleWithFixedDelay(() -> scanTxLogTable(), txSweeperFixedDelaySeconds, txSweeperFixedDelaySeconds, TimeUnit.SECONDS);
        scheduledExecutorService.scheduleWithFixedDelay(() -> executeTxSweeperTasks(), 1L, 1L, TimeUnit.SECONDS);
    }

    protected void executeTxSweeperTasks() {
        try {
            seenTransactions.values().forEach(txSweeperTask -> {
                boolean isTxSweeperTaskComplete = txSweeperTask.execute();
                if (isTxSweeperTaskComplete) {
                    seenTransactions.remove(txSweeperTask.getTransactionId());
                }
            });
        } catch (Exception e) {
            LOG.error("Exception occurred while executing sweeper tasks.", e);
        }
    }

    protected void scanTxLogTable() {
        try {
            ScanRequest scanRequest = new ScanRequest().withTableName(TransactionLogItemMapper.TRANSACTION_LOG_TABLE_NAME).withSelect(Select.ALL_ATTRIBUTES)
                    .withLimit(txLogTableScanPageSize);
            ScanResult result;
            Map<String, AttributeValue> exclusiveStartKey = null;
            do {
                result = ddbClient.scan(scanRequest.withExclusiveStartKey(exclusiveStartKey));
                LOG.info(String.format("Found %d transactions to sweep", result.getCount()));
                for (Map<String, AttributeValue> item : result.getItems()) {
                    if (Thread.currentThread().isInterrupted()) {
                        LOG.debug("Thread is interrupted. Stop sweeping");
                        break;
                    }
                    TransactionLogItem txLogItem = txLogItemMapper.unmarshall(item);
                    seenTransactions.putIfAbsent(txLogItem.getTransactionId(), new TransactionSweeperTask(txLogItem, Instant.now(), ddbClient, txLogItemMapper));
                }
                exclusiveStartKey = result.getLastEvaluatedKey();
            } while (exclusiveStartKey != null);
        } catch (Exception e) {
            LOG.error(String.format("Exception occured while scanning %s table", TransactionLogItemMapper.TRANSACTION_LOG_TABLE_NAME), e);
        }
    }

    protected Map<String, TransactionSweeperTask> getSeenTransactions() {
        return seenTransactions;
    }
}
