package com.github.paleblue.persistence.milkha;

import static com.github.paleblue.persistence.milkha.util.Preconditions.checkNotNull;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.github.paleblue.persistence.milkha.dto.TransactionLogItem;
import com.github.paleblue.persistence.milkha.dto.TransactionStatus;
import com.github.paleblue.persistence.milkha.exception.UnexpectedTransactionStateException;
import com.github.paleblue.persistence.milkha.mapper.TransactionLogItemMapper;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TransactionSweeperTask {
    private static final Log LOG = LogFactory.getLog(TransactionSweeperTask.class);

    private final Instant txFirstSeenAt;
    private final AmazonDynamoDB ddbClient;
    private final TransactionLogItemMapper txLogItemMapper;
    private final String txId;
    private final long waitPeriodBeforeSweeperDeleteMillis;
    private final long waitPeriodBeforeSweeperUnlockMillis;

    public TransactionSweeperTask(final TransactionLogItem txLogItem, final Instant txFirstSeenAt,
            final AmazonDynamoDB ddbClient, final TransactionLogItemMapper txLogItemMapper) {
        checkNotNull(txLogItem);
        this.waitPeriodBeforeSweeperDeleteMillis = txLogItem.getWaitPeriodBeforeSweeperDeleteMillis();
        this.waitPeriodBeforeSweeperUnlockMillis = txLogItem.getWaitPeriodBeforeSweeperUnlockMillis();
        this.txId = txLogItem.getTransactionId();
        this.txFirstSeenAt = checkNotNull(txFirstSeenAt);
        this.ddbClient = checkNotNull(ddbClient);
        this.txLogItemMapper = checkNotNull(txLogItemMapper);
    }

    public String getTransactionId() {
        return this.txId;
    }

    public boolean execute() {
        if (isWaitPeriodBeforeSweeperUnlockElapsed()) {
            unlockTransaction();
            if (isWaitPeriodBeforeSweeperDeleteElapsed()) {
                deleteTransaction();
                return true;
            }
        }
        return false;
    }

    private TransactionLogItem getTxLogItem() {
        Map<String, AttributeValue> rawTxLogItem = ddbClient.getItem(txLogItemMapper.generateGetItemRequest(this.txId)).getItem();
        if (rawTxLogItem == null) {
            return null;
        } else {
            return txLogItemMapper.unmarshall(rawTxLogItem);
        }
    }

    private void deleteTransaction() {
        try {
            TransactionLogItem txLogItem = getTxLogItem();
            if (txLogItem != null) {
                ddbClient.deleteItem(txLogItemMapper.generateDeleteItemRequest(this.txId));
                LOG.info(String.format("Deleted transaction [%s].", this.txId));
            }
        } catch (Exception e) {
            LOG.warn(String.format("Transaction delete failed [%s].", this.txId), e);
            throw e;
        }
    }

    private void unlockTransaction() {
        try {
            TransactionLogItem txLogItem = getTxLogItem();
            if (txLogItem != null && !txLogItem.isUnlockedBySweeper()) {
                LOG.info(String.format("Unlocking transaction [%s]", this.txId));
                TransactionRequestsFactory txRequestsFactory = new TransactionRequestsFactory(txLogItem);
                List<AmazonWebServiceRequest> unlockRequests;
                switch (txLogItem.getTransactionStatus()) {
                case COMMITTED:
                    unlockRequests = txRequestsFactory.generatePostCommitUnlockRequestsFromCommitSets();
                    break;
                case ROLLED_BACK:
                    unlockRequests = txRequestsFactory.generatePostRollbackUnlockRequestsFromCommitSets();
                    break;
                case START_COMMIT:
                    txLogItem.setTransactionStatus(TransactionStatus.ROLLED_BACK);
                    ddbClient.putItem(txRequestsFactory.generatePutRequestForTransactionLogItem());
                    txLogItem.setTransactionStatus(TransactionStatus.ROLLED_BACK); // Needed for previous status to update correctly
                    unlockRequests = txRequestsFactory.generatePostRollbackUnlockRequestsFromCommitSets();
                    break;
                default:
                    // If we got here, then the given transaction is not a pristine DDB read
                    throw new UnexpectedTransactionStateException(String.format("TransactionId [%s], Status [%s]", this.txId, txLogItem.getTransactionStatus().name()));
                }
                executeRequests(unlockRequests); // Not using a thread-pool for simplicity. The sweep is already executing as a FutureTask.
                txLogItem.setUnlockedBySweeper(true);
                ddbClient.putItem(txRequestsFactory.generatePutRequestForTransactionLogItem());
                LOG.info(String.format("Unlocked transaction [%s].", this.txId));
            }
        } catch (Exception e) {
            LOG.warn(String.format("Transaction unlock failed [%s].", this.txId), e);
            throw e;
        }
    }

    private void executeRequests(List<AmazonWebServiceRequest> requests) {
        requests.stream().forEach(request -> {
            try {
                if (request instanceof DeleteItemRequest) {
                    ddbClient.deleteItem((DeleteItemRequest) request);
                } else if (request instanceof UpdateItemRequest) {
                    ddbClient.updateItem((UpdateItemRequest) request);
                } else {
                    throw new UnsupportedOperationException("Only delete and update requests are supported.");
                }
            } catch (ConditionalCheckFailedException e) {
                LOG.info(String.format("Conditional check failed while unlocking item."
                        + " This means the item was already unlocked. Request: %s", request));
            }
        });
    }

    private boolean isWaitPeriodBeforeSweeperDeleteElapsed() {
        return Instant.now().isAfter(txFirstSeenAt.plusMillis(this.waitPeriodBeforeSweeperDeleteMillis));
    }

    private boolean isWaitPeriodBeforeSweeperUnlockElapsed() {
        return Instant.now().isAfter(txFirstSeenAt.plusMillis(this.waitPeriodBeforeSweeperUnlockMillis));
    }
}
