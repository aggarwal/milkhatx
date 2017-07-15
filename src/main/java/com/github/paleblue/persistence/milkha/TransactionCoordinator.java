package com.github.paleblue.persistence.milkha;

import static com.github.paleblue.persistence.milkha.util.Preconditions.checkArgument;
import static com.github.paleblue.persistence.milkha.util.Preconditions.checkNotNull;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.github.paleblue.persistence.milkha.dto.TransactionLogItem;
import com.github.paleblue.persistence.milkha.dto.TransactionStatus;
import com.github.paleblue.persistence.milkha.exception.ContentionException;
import com.github.paleblue.persistence.milkha.exception.TransactionNotStartedException;
import com.github.paleblue.persistence.milkha.exception.TransactionPendingException;
import com.github.paleblue.persistence.milkha.mapper.HashOnlyMapper;
import com.github.paleblue.persistence.milkha.mapper.TransactionLogItemMapper;
import com.github.paleblue.persistence.milkha.util.Futures;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public final class TransactionCoordinator {

    private static final Log LOG = LogFactory.getLog(TransactionCoordinator.class);

    protected static final String TRANSACTION_ID_CONTROL_FIELD = "_TxId";
    protected static final String TRANSACTION_OPERATION_CONTROL_FIELD = "_TxOperation";
    protected static final String TRANSACTION_OPERATION_ADD_VALUE = "ADD";
    protected static final String TRANSACTION_OPERATION_DELETE_VALUE = "DELETE";
    protected static final List<String> TRANSACTION_CONTROL_FIELDS = Arrays.asList(TRANSACTION_ID_CONTROL_FIELD, TRANSACTION_OPERATION_CONTROL_FIELD);

    private final AmazonDynamoDB ddbClient;
    private final HashOnlyMapper<TransactionLogItem> txLogItemMapper;
    private final ExecutorService executorService;
    private final long maxTimeToCommitOrRollbackMillis;
    private final long waitPeriodBeforeSweeperUnlockMillis;
    private final long waitPeriodBeforeSweeperDeleteMillis;

    private TransactionLogItem txLogItem;
    private TransactionRequestsFactory txRequestsFactory;
    private List<UpdateItemRequest> stagedUpdatesForCommit;
    private List<DeleteItemRequest> stagedDeletesForPostRollbackUnlock;
    private List<UpdateItemRequest> stagedUpdatesForPostRollbackUnlock;
    private List<UpdateItemRequest> stagedUpdatesForPostCommitUnlock;
    private List<DeleteItemRequest> stagedDeletesForPostCommitUnlock;

    TransactionCoordinator(AmazonDynamoDB ddbClient, ExecutorService executorService, long maxTimeToCommitOrRollbackMillis,
            long waitPeriodBeforeSweeperUnlockMillis, long waitPeriodBeforeSweeperDeleteMillis) {
        checkArgument(maxTimeToCommitOrRollbackMillis > 0, "maxTimeToCommitOrRollbackMillis must be greater than zero");
        checkArgument(waitPeriodBeforeSweeperUnlockMillis > maxTimeToCommitOrRollbackMillis, "waitPeriodBeforeSweeperUnlockMillis must be greater than maxTimeToCommitOrRollbackMillis");
        checkArgument(waitPeriodBeforeSweeperDeleteMillis > waitPeriodBeforeSweeperUnlockMillis, "waitPeriodBeforeSweeperDeleteMillis must be greater than waitPeriodBeforeSweeperUnlockMillis");
        this.ddbClient = checkNotNull(ddbClient);
        this.executorService = checkNotNull(executorService);
        this.txLogItemMapper = new TransactionLogItemMapper();
        this.maxTimeToCommitOrRollbackMillis = maxTimeToCommitOrRollbackMillis;
        this.waitPeriodBeforeSweeperUnlockMillis = waitPeriodBeforeSweeperUnlockMillis;
        this.waitPeriodBeforeSweeperDeleteMillis = waitPeriodBeforeSweeperDeleteMillis;
    }

    public void createItem(UpdateItemRequest updateItemRequest) {
        assertTransactionStarted();
        txLogItem.addToCreateSet(updateItemRequest.getTableName(), updateItemRequest.getKey());
        UpdateItemRequest preparedUpdateItemRequest = txRequestsFactory.generateCommitRequestForAdd(updateItemRequest);
        stagedUpdatesForCommit.add(preparedUpdateItemRequest);
        stagedUpdatesForPostCommitUnlock.add(txRequestsFactory.generatePostCommitUnlockRequestForAdd(preparedUpdateItemRequest));
        stagedDeletesForPostRollbackUnlock.add(txRequestsFactory.generatePostRollbackUnlockRequestForAdd(preparedUpdateItemRequest));
    }

    public void deleteItem(DeleteItemRequest deleteItemRequest) {
        assertTransactionStarted();
        txLogItem.addToDeleteSet(deleteItemRequest.getTableName(), deleteItemRequest.getKey());
        UpdateItemRequest preparedUpdateItemRequest = txRequestsFactory.generateCommitRequestForDelete(deleteItemRequest);
        stagedUpdatesForCommit.add(preparedUpdateItemRequest);
        stagedDeletesForPostCommitUnlock.add(txRequestsFactory.generatePostCommitUnlockRequestForDelete(preparedUpdateItemRequest));
        stagedUpdatesForPostRollbackUnlock.add(txRequestsFactory.generatePostRollbackUnlockRequestForDelete(preparedUpdateItemRequest));
    }

    public QueryResult query(QueryRequest queryRequest) {
        QueryResult result = ddbClient.query(queryRequest);
        List<Map<String, AttributeValue>> allItems = result.getItems();
        List<Map<String, AttributeValue>> visibleItems = isolateCommittedItems(allItems);
        removeControlFields(visibleItems);
        result.setItems(visibleItems);
        return result;
    }

    public ScanResult scan(ScanRequest scanRequest) {
        ScanResult result = ddbClient.scan(scanRequest);
        List<Map<String, AttributeValue>> allItems = result.getItems();
        List<Map<String, AttributeValue>> visibleItems = isolateCommittedItems(allItems);
        removeControlFields(visibleItems);
        result.setItems(visibleItems);
        return result;
    }

    private List<Map<String, AttributeValue>> isolateCommittedItems(List<Map<String, AttributeValue>> rawItems) {
        // Separate locked and unlocked item.
        List<Map<String, AttributeValue>> unlockedItems = new ArrayList<>();
        List<Map<String, AttributeValue>> lockedItems = new ArrayList<>();
        for (Map<String, AttributeValue> item : rawItems) {
            if (item.containsKey(TRANSACTION_ID_CONTROL_FIELD)) {
                lockedItems.add(item);
            } else {
                unlockedItems.add(item);
            }
        }

        // Select all locked items that should be surfaced to the user based on transaction state
        unlockedItems.addAll(selectVisibleItems(lockedItems));
        return unlockedItems;
    }

    private void removeControlFields(List<Map<String, AttributeValue>> committedItems) {
        for (Map<String, AttributeValue> committedItem : committedItems) {
            committedItem.remove(TRANSACTION_ID_CONTROL_FIELD);
            committedItem.remove(TRANSACTION_OPERATION_CONTROL_FIELD);
        }
    }

    private List<Map<String, AttributeValue>> selectVisibleItems(List<Map<String, AttributeValue>> lockedItems) {
        List<Map<String, AttributeValue>> visibleItems = new ArrayList<>();
        if (lockedItems.size() == 0) {
            return  visibleItems;
        }

        // Build a map of TableName -> Primary Key to batch lookup TransactionLogItem records
        Map<String, KeysAndAttributes> tableToKeysAndAttributesMap = new HashMap<>();
        Map<String, Map<String, AttributeValue>> txIdToPrimaryKeyMap = new HashMap<>();
        for (Map<String, AttributeValue> lockedItem : lockedItems) {
            String parentTransactionId = lockedItem.get(TRANSACTION_ID_CONTROL_FIELD).getS();
            txIdToPrimaryKeyMap.put(parentTransactionId, txLogItemMapper.getPrimaryKeyMap(parentTransactionId));
        }
        tableToKeysAndAttributesMap.put(txLogItemMapper.getTableName(), new KeysAndAttributes().withConsistentRead(true).withKeys(txIdToPrimaryKeyMap.values()));

        // Execute the Batch lookup request and collect raw TransactionLogItem records
        List<Map<String, AttributeValue>> rawParentTxLogItems = new ArrayList<>();
        while (!tableToKeysAndAttributesMap.isEmpty()) {
            BatchGetItemResult result = ddbClient.batchGetItem(tableToKeysAndAttributesMap);
            rawParentTxLogItems.addAll(result.getResponses().get(txLogItemMapper.getTableName()));
            tableToKeysAndAttributesMap = result.getUnprocessedKeys();
        }

        // Build a map of transactionId -> transactionStatus for easy lookup
        Map<String, TransactionStatus> txIdToStatusMap = new HashMap<>(rawParentTxLogItems.size());
        for (Map<String, AttributeValue> rawParentTxLogItem : rawParentTxLogItems) {
            TransactionLogItem parentTxLogItem = txLogItemMapper.unmarshall(rawParentTxLogItem);
            txIdToStatusMap.put(parentTxLogItem.getTransactionId(), parentTxLogItem.getTransactionStatus());
        }

        // Based on parent transaction status, surface eligible items as visible
        for (Map<String, AttributeValue> lockedItem : lockedItems) {
            String pendingOperation = lockedItem.get(TRANSACTION_OPERATION_CONTROL_FIELD).getS();
            TransactionStatus parentTxStatus = txIdToStatusMap.get(lockedItem.get(TRANSACTION_ID_CONTROL_FIELD).getS());
            if (TRANSACTION_OPERATION_ADD_VALUE.equals(pendingOperation) && TransactionStatus.COMMITTED.equals(parentTxStatus)) {
                visibleItems.add(lockedItem);
            } else if (TRANSACTION_OPERATION_DELETE_VALUE.equals(pendingOperation) && !TransactionStatus.COMMITTED.equals(parentTxStatus)) {
                visibleItems.add(lockedItem);
            }
        }
        return visibleItems;
    }

    public void startTransaction() {
        startTransaction(String.valueOf(UUID.randomUUID()));
    }

    protected void startTransaction(String transactionId) {
        if (isTransactionComplete()) {
            txLogItem = new TransactionLogItem(transactionId, TransactionStatus.NOT_PERSISTED, waitPeriodBeforeSweeperUnlockMillis, waitPeriodBeforeSweeperDeleteMillis);
            txRequestsFactory = new TransactionRequestsFactory(txLogItem);
            stagedUpdatesForCommit = new ArrayList<>();
            stagedUpdatesForPostCommitUnlock = new ArrayList<>();
            stagedDeletesForPostCommitUnlock = new ArrayList<>();
            stagedDeletesForPostRollbackUnlock = new ArrayList<>();
            stagedUpdatesForPostRollbackUnlock = new ArrayList<>();
        } else {
            throw new TransactionPendingException("A transaction is already in flight");
        }
    }

    public List<Future> commit() {
        assertTransactionStarted();
        commitWithoutUnlocking();
        return executePostCommitUnlocks();
    }

    protected List<Future> executePostCommitUnlocks() {
        return executeUnlockRequests(stagedUpdatesForPostCommitUnlock, stagedDeletesForPostCommitUnlock);
    }

    protected void commitWithoutUnlocking() {
        assertTransactionStarted();
        Instant endCommitAtTime = Instant.now().plusMillis(maxTimeToCommitOrRollbackMillis);
        txLogItem.setTransactionStatus(TransactionStatus.START_COMMIT);
        persistTransactionLogItem(endCommitAtTime);
        List<Future> updatesForCommitFutures = executeRequests(stagedUpdatesForCommit);
        Futures.blockOnAllFutures(updatesForCommitFutures, endCommitAtTime, new ContentionException("Item pending commit or cleanup. Cannot proceed with commit."));
        try {
            txLogItem.setTransactionStatus(TransactionStatus.COMMITTED);
            persistTransactionLogItem(endCommitAtTime);
        } catch (AmazonClientException e) { // Allow user to rollback
            txLogItem.setTransactionStatus(TransactionStatus.START_COMMIT);
            throw e;
        }
        txLogItem.setTransactionStatus(TransactionStatus.COMPLETE);
    }

    public List<Future> rollback() {
        assertTransactionStarted();
        List<Future> postRollbackUnlockFutures = Collections.singletonList(new FutureTask(() -> null));
        if (txLogItem.getTransactionStatus() != TransactionStatus.NOT_PERSISTED) {
            rollbackWithoutUnlocking();
            postRollbackUnlockFutures = executePostRollbackUnlock();
        }
        txLogItem.setTransactionStatus(TransactionStatus.COMPLETE);
        return postRollbackUnlockFutures;
    }

    private List<Future> executePostRollbackUnlock() {
        return executeUnlockRequests(stagedUpdatesForPostRollbackUnlock, stagedDeletesForPostRollbackUnlock);
    }

    protected void rollbackWithoutUnlocking() {
        assertTransactionStarted();
        if (txLogItem.getTransactionStatus() != TransactionStatus.NOT_PERSISTED) {
            txLogItem.setTransactionStatus(TransactionStatus.ROLLED_BACK);
            Instant rollbackStartTime = Instant.now();
            persistTransactionLogItem(rollbackStartTime.plusMillis(maxTimeToCommitOrRollbackMillis));
        }
        txLogItem.setTransactionStatus(TransactionStatus.COMPLETE);
    }

    private List<Future> executeUnlockRequests(List<UpdateItemRequest> updateRequests, List<DeleteItemRequest> deleteItemRequests) {
        List<AmazonWebServiceRequest> requests = new ArrayList<>(updateRequests.size() + deleteItemRequests.size());
        requests.addAll(updateRequests);
        requests.addAll(deleteItemRequests);
        return executeRequests(requests);
    }

    private List<Future> executeRequests(List requests) {
        List<Future> futures = new ArrayList<>(requests.size());
        for (final Object request : requests) {
            futures.add(executorService.submit(() -> {
                final long startTime = System.nanoTime();
                try {
                    if (request instanceof DeleteItemRequest) {
                        executeRequest((DeleteItemRequest) request);
                    } else if (request instanceof UpdateItemRequest) {
                        executeRequest((UpdateItemRequest) request);
                    } else {
                        throw new UnsupportedOperationException("Only delete and update requests are supported.");
                    }
                } finally {
                    logIfDebugEnabled(System.nanoTime() - startTime, request);
                }
            }));
        }
        return futures;
    }

    private void executeRequest(DeleteItemRequest request) {
        try {
            ddbClient.deleteItem(request);
        } catch (AmazonServiceException e) {
            LOG.warn(String.format("DeleteItemRequest failed: %s", request));
            throw e;
        }
    }

    private void executeRequest(UpdateItemRequest request) {
        try {
            ddbClient.updateItem(request);
        } catch (AmazonServiceException e) {
            LOG.warn(String.format("UpdateItemRequest failed: %s", request));
            throw e;
        }
    }

    private void executeRequest(PutItemRequest request) {
        try {
            ddbClient.putItem(request);
        } catch (AmazonServiceException e) {
            LOG.warn(String.format("PutItemRequest failed: %s", request));
            throw e;
        }
    }

    private void logIfDebugEnabled(long nanoSecond, Object request) {
        logIfDebugEnabled("Duration = " + TimeUnit.NANOSECONDS.toMillis(nanoSecond) + " ms", request);
    }

    private void logIfDebugEnabled(String message, Object request) {
        // Check log level first to avoid the overhead of serialization in case we don't log anything
        if (LOG.isDebugEnabled()) {
            LOG.debug(message + ", Request = " + request.toString());
        }
    }

    private void persistTransactionLogItem(Instant endTime) {
        PutItemRequest request = txRequestsFactory.generatePutRequestForTransactionLogItem();
        Future txLogItemFuture = executorService.submit(() -> executeRequest(request));
        Futures.blockOnAllFutures(Arrays.asList(txLogItemFuture), endTime, new ContentionException("Experienced contention with another coordinator."));
    }

    protected void reloadTransactionLogItem() {
        GetItemResult result = ddbClient.getItem(txLogItemMapper.generateGetItemRequest(txLogItem.getTransactionId()));
        txLogItem = txLogItemMapper.unmarshall(result.getItem());
        txRequestsFactory = new TransactionRequestsFactory(txLogItem);
    }

    private void assertTransactionStarted() {
        if (isTransactionComplete()) {
            throw new TransactionNotStartedException("Cannot proceed unless transaction is started");
        }
    }

    private boolean isTransactionComplete() {
        return txLogItem == null || txLogItem.getTransactionStatus() == TransactionStatus.COMPLETE;
    }
}
