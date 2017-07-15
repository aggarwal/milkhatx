package com.github.paleblue.persistence.milkha;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.github.paleblue.persistence.milkha.dto.BankAccountItem;
import com.github.paleblue.persistence.milkha.dto.TransactionStatus;
import com.github.paleblue.persistence.milkha.exception.ContentionException;
import com.github.paleblue.persistence.milkha.exception.TransactionNotStartedException;
import com.github.paleblue.persistence.milkha.mapper.BankAccountItemMapper;
import com.github.paleblue.persistence.milkha.mapper.TransactionLogItemMapper;
import com.github.paleblue.persistence.milkha.util.Futures;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;

import org.junit.Before;
import org.junit.Test;


public class TransactionCoordinatorItemPersistenceTest extends TransactionCoordinatorBaseTest {

    private BankAccountItem drEvilSavingsAccount;

    @Before
    public void setup() {
        String beneficiaryName = String.format("DrEvil-%s", UUID.randomUUID().toString());
        Integer usdAmount = random.nextInt(1000000);
        drEvilSavingsAccount = new BankAccountItem(beneficiaryName, SAVINGS_ACCOUNT_TYPE, usdAmount);
    }

    private Map<String, AttributeValue> getDrEvilSavingsItemFromDynamo() {
        GetItemRequest request = bankAccountItemMapper.
                generateGetItemRequest(drEvilSavingsAccount.getBeneficiaryName(), drEvilSavingsAccount.getAccountType());
        List<String> attributesToGet = request.getAttributesToGet();
        attributesToGet.addAll(TransactionCoordinator.TRANSACTION_CONTROL_FIELDS);
        return ddbClient.getItem(request).getItem();
    }

    private QueryRequest getDrEvilSavingsQueryRequest() {
        return bankAccountItemMapper.generateQueryRequest(drEvilSavingsAccount.getBeneficiaryName(),
                drEvilSavingsAccount.getAccountType());
    }

    private DeleteItemRequest getDrEvilSavingsDeleteItemRequest() {
        return bankAccountItemMapper.generateDeleteItemRequest(drEvilSavingsAccount.getBeneficiaryName(),
                drEvilSavingsAccount.getAccountType());
    }

    @Test
    public void whenCommitWithoutUnlockIsInvokedAfterCreateThenItemIsPersistedWithCorrectControlAttributes() {
        String transactionId = randomTransactionId();
        createItemWithoutUnlockingCommit(transactionId, drEvilSavingsAccount);
        Map<String, AttributeValue> ddbItemMap = getDrEvilSavingsItemFromDynamo();
        assertEquals(transactionId, ddbItemMap.get(TransactionCoordinator.TRANSACTION_ID_CONTROL_FIELD).getS());
        assertEquals(TransactionCoordinator.TRANSACTION_OPERATION_ADD_VALUE, ddbItemMap.get(TransactionCoordinator.TRANSACTION_OPERATION_CONTROL_FIELD).getS());
    }

    @Test
    public void whenCommitIsInvokedControlFieldsAreCleared() {
        String transactionId = randomTransactionId();
        createItemWithUnlockingCommit(transactionId, drEvilSavingsAccount);
        Map<String, AttributeValue> itemMap = getDrEvilSavingsItemFromDynamo();
        assertNull(itemMap.get(TransactionCoordinator.TRANSACTION_ID_CONTROL_FIELD));
        assertNull(itemMap.get(TransactionCoordinator.TRANSACTION_OPERATION_CONTROL_FIELD));
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenUpdateItemRequestHasConditionalExpressionSetThenThrowException() {
        coordinator.startTransaction();
        UpdateItemRequest updateItemRequest = bankAccountItemMapper.generateUpdateItemRequest(drEvilSavingsAccount);
        updateItemRequest.setConditionExpression(String.format("%s > 999999", BankAccountItemMapper.TOTAL_AMOUNT_IN_USD_KEY_NAME));
        coordinator.createItem(updateItemRequest);
    }

    @Test(expected = ContentionException.class)
    public void whenCreatingAnExistingItemThenThrowException() {
        coordinator.startTransaction();
        coordinator.createItem(bankAccountItemMapper.generateUpdateItemRequest(drEvilSavingsAccount));
        coordinator.commitWithoutUnlocking();
        coordinator.startTransaction();
        coordinator.createItem(bankAccountItemMapper.generateUpdateItemRequest(drEvilSavingsAccount));
        coordinator.commitWithoutUnlocking();
    }

    @Test
    public void itemNotPersistedToDynamoOncreateUnlessTransactionCommitInvoked() {
        coordinator.startTransaction();
        coordinator.createItem(bankAccountItemMapper.generateUpdateItemRequest(drEvilSavingsAccount));
        assertNull(getDrEvilSavingsItemFromDynamo());
    }

    @Test(expected = TransactionNotStartedException.class)
    public void whenCreateIsInvokedWithoutStartingTransactionThenThrowException() {
        coordinator.createItem(bankAccountItemMapper.generateUpdateItemRequest(drEvilSavingsAccount));
    }

    @Test(expected = TransactionNotStartedException.class)
    public void whenDeleteIsInvokedWithoutStartingTransactionThenThrowException() {
        coordinator.deleteItem(getDrEvilSavingsDeleteItemRequest());
    }

    @Test
    public void whenCommitWithoutUnlockIsInvokedAfterDeleteThenItemIsUpdatedWithCorrectControlAttributes() {
        createItemWithUnlockingCommit(drEvilSavingsAccount);
        String transactionId = randomTransactionId();
        coordinator.startTransaction(transactionId);
        coordinator.deleteItem(getDrEvilSavingsDeleteItemRequest());
        coordinator.commitWithoutUnlocking();
        Map<String, AttributeValue> ddbItemMap = getDrEvilSavingsItemFromDynamo();
        assertEquals(transactionId, ddbItemMap.get(TransactionCoordinator.TRANSACTION_ID_CONTROL_FIELD).getS());
        assertEquals(TransactionCoordinator.TRANSACTION_OPERATION_DELETE_VALUE, ddbItemMap.get(TransactionCoordinator.TRANSACTION_OPERATION_CONTROL_FIELD).getS());
    }

    @Test(expected = ContentionException.class)
    public void whenDeletingPendingItemThenThrowContentionException() {
        coordinator.startTransaction();
        coordinator.createItem(bankAccountItemMapper.generateUpdateItemRequest(drEvilSavingsAccount));
        coordinator.commitWithoutUnlocking();
        coordinator.startTransaction();
        coordinator.deleteItem(getDrEvilSavingsDeleteItemRequest());
        coordinator.commitWithoutUnlocking();
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenUserSetsConditionExpressionDuringDeleteThenThrowException() {
        coordinator.startTransaction();
        DeleteItemRequest deleteItemRequest = getDrEvilSavingsDeleteItemRequest();
        Map<String, String> expressionAttributeName = new HashMap<>(1);
        expressionAttributeName.put("#usdAmount", BankAccountItemMapper.TOTAL_AMOUNT_IN_USD_KEY_NAME);
        Map<String, AttributeValue> expressionAttributeValue = new HashMap<>(1);
        expressionAttributeValue.put(":usdAmount", new AttributeValue().withN("1000000"));
        deleteItemRequest.setExpressionAttributeNames(expressionAttributeName);
        deleteItemRequest.setExpressionAttributeValues(expressionAttributeValue);
        deleteItemRequest.setConditionExpression("#usdAmount > :usdAmount");
        coordinator.deleteItem(deleteItemRequest);
    }

    @Test
    public void whenNoConditionExpressionSpecifiedByUserInDeleteItemRequestThenSucceed() {
        createItemWithUnlockingCommit(drEvilSavingsAccount);
        coordinator.startTransaction();
        coordinator.deleteItem(getDrEvilSavingsDeleteItemRequest());
        coordinator.commitWithoutUnlocking();
    }

    @Test
    public void deleteItemDoesNotUpdateItemInDynamoBeforeCommitIsInvoked() {
        createItemWithUnlockingCommit(drEvilSavingsAccount);
        coordinator.startTransaction();
        coordinator.deleteItem(getDrEvilSavingsDeleteItemRequest());
        Map<String, AttributeValue> ddbItemMap = getDrEvilSavingsItemFromDynamo();
        assertNull(ddbItemMap.get(TransactionCoordinator.TRANSACTION_ID_CONTROL_FIELD));
        assertNull(ddbItemMap.get(TransactionCoordinator.TRANSACTION_OPERATION_CONTROL_FIELD));
    }

    @Test
    public void commitDeletesItemFromDynamo() throws ExecutionException, InterruptedException {
        createItemWithUnlockingCommit(drEvilSavingsAccount);
        coordinator.startTransaction();
        coordinator.deleteItem(getDrEvilSavingsDeleteItemRequest());
        List<Future> unlockFutures = coordinator.commit();
        Futures.blockOnAllFutures(unlockFutures, Instant.now().plusSeconds(1));
        assertNull(getDrEvilSavingsItemFromDynamo());
    }

    @Test(expected = ContentionException.class)
    public void whenItemIsAlreadyMarkedForDeletionThenThrowContentionException() throws ExecutionException, InterruptedException {
        createItemWithUnlockingCommit(drEvilSavingsAccount);
        coordinator.startTransaction();
        coordinator.deleteItem(getDrEvilSavingsDeleteItemRequest());
        coordinator.commitWithoutUnlocking();
        coordinator.startTransaction();
        coordinator.deleteItem(getDrEvilSavingsDeleteItemRequest());
        List<Future> unlockFutures = coordinator.commit();
        Futures.blockOnAllFutures(unlockFutures, Instant.now().plusSeconds(1));
    }

    @Test(expected = ContentionException.class)
    public void whenDeletingNonExistentItemThenThrowException() {
        coordinator.startTransaction();
        coordinator.deleteItem(getDrEvilSavingsDeleteItemRequest());
        coordinator.commitWithoutUnlocking();
    }

    @Test
    public void whenAddedItemIsCommittedAndUnlockedThenUserSeesItem() {
        createItemWithUnlockingCommit(drEvilSavingsAccount);
        QueryResult queryResult = coordinator.query(getDrEvilSavingsQueryRequest());
        List<Map<String, AttributeValue>> committedItems = queryResult.getItems();
        assertEquals(1, committedItems.size());
        Map<String, AttributeValue> committedItem = committedItems.get(0);
        assertEquals(drEvilSavingsAccount.getBeneficiaryName(), committedItem.get(BankAccountItemMapper.ACCOUNT_BENEFICIARY_KEY_NAME).getS());
        assertEquals(drEvilSavingsAccount.getTotalAmountInUsd(), new Integer(committedItem.get(BankAccountItemMapper.TOTAL_AMOUNT_IN_USD_KEY_NAME).getN()));
    }

    @Test
    public void whenAddedItemIsCommittedButLockedThenUserSeesItem() {
        coordinator.startTransaction();
        coordinator.createItem(bankAccountItemMapper.generateUpdateItemRequest(drEvilSavingsAccount));
        coordinator.commitWithoutUnlocking();
        QueryResult queryResult = coordinator.query(getDrEvilSavingsQueryRequest());
        List<Map<String, AttributeValue>> committedItems = queryResult.getItems();
        assertEquals(1, committedItems.size());
        Map<String, AttributeValue> committedItem = committedItems.get(0);
        assertEquals(drEvilSavingsAccount.getBeneficiaryName(), committedItem.get(BankAccountItemMapper.ACCOUNT_BENEFICIARY_KEY_NAME).getS());
        assertEquals(drEvilSavingsAccount.getTotalAmountInUsd(), new Integer(committedItem.get(BankAccountItemMapper.TOTAL_AMOUNT_IN_USD_KEY_NAME).getN()));
    }

    @Test
    public void whenDeletedItemIsCommittedAndUnlockedThenUserDoesNotSeeItem() throws ExecutionException, InterruptedException {
        createItemWithUnlockingCommit(drEvilSavingsAccount);
        QueryResult queryResult = coordinator.query(getDrEvilSavingsQueryRequest());
        assertEquals(1, queryResult.getItems().size());
        coordinator.startTransaction();
        coordinator.deleteItem(getDrEvilSavingsDeleteItemRequest());
        List<Future> unlockFutures = coordinator.commit();
        Futures.blockOnAllFutures(unlockFutures, Instant.now().plusSeconds(1));
        queryResult = coordinator.query(getDrEvilSavingsQueryRequest());
        List<Map<String, AttributeValue>> committedItems = queryResult.getItems();
        assertEquals(0, committedItems.size());
    }

    @Test
    public void whenDeletedItemIsCommittedButLockedThenUserDoesNotSeeItem() {
        createItemWithUnlockingCommit(drEvilSavingsAccount);
        QueryResult queryResult = coordinator.query(getDrEvilSavingsQueryRequest());
        assertEquals(1, queryResult.getItems().size());
        coordinator.startTransaction();
        coordinator.deleteItem(getDrEvilSavingsDeleteItemRequest());
        coordinator.commitWithoutUnlocking();
        queryResult = coordinator.query(getDrEvilSavingsQueryRequest());
        List<Map<String, AttributeValue>> committedItems = queryResult.getItems();
        assertEquals(0, committedItems.size());
    }

    @Test
    public void itemsInQueryResultDoNotHaveControlAttributes() {
        createItemWithUnlockingCommit(drEvilSavingsAccount);
        QueryResult queryResult = coordinator.query(getDrEvilSavingsQueryRequest());
        List<Map<String, AttributeValue>> committedItems = queryResult.getItems();
        assertEquals(1, committedItems.size());
        Map<String, AttributeValue> committedItem = committedItems.get(0);
        assertFalse(committedItem.containsKey(TransactionCoordinator.TRANSACTION_ID_CONTROL_FIELD));
        assertFalse(committedItem.containsKey(TransactionCoordinator.TRANSACTION_OPERATION_CONTROL_FIELD));
        for (String controlField : TransactionCoordinator.TRANSACTION_CONTROL_FIELDS) { // In case there are more fields in the future
            assertFalse(committedItem.containsKey(controlField));
        }
    }

    @Test
    public void whenDynamoQueryReturnsNothingThenNoItemsAreReturnedInResult() {
        QueryResult queryResult = coordinator.query(getDrEvilSavingsQueryRequest());
        assertTrue(queryResult.getItems().isEmpty());
    }

    @Test
    public void queryMethodDoesNotThrowExceptionIfTransactionIsNotStarted() {
        coordinator.query(getDrEvilSavingsQueryRequest());
    }

    private void whenAddedItemIsPersistedButNotCommittedThenUserDoesNotSeeItem(TransactionStatus txStatusOverride) {
        String transactionId = randomTransactionId();
        createItemWithoutUnlockingCommit(transactionId, drEvilSavingsAccount);
        QueryRequest queryRequest = getDrEvilSavingsQueryRequest();
        QueryResult queryResult = coordinator.query(queryRequest);
        assertEquals(1, queryResult.getItems().size());
        overrideTransactionStatus(transactionId, txStatusOverride);
        queryRequest = getDrEvilSavingsQueryRequest();
        queryResult = coordinator.query(queryRequest);
        assertEquals(0, queryResult.getItems().size());
    }

    @Test
    public void whenAddedItemIsPersistedAndParentTransactionIsStartedThenUserDoesNotSeeItem() {
        whenAddedItemIsPersistedButNotCommittedThenUserDoesNotSeeItem(TransactionStatus.START_COMMIT);
    }

    @Test
    public void whenAddedItemIsPersistedAndParentTransactionIsRolledBackThenUserDoesNotSeeItem() {
        whenAddedItemIsPersistedButNotCommittedThenUserDoesNotSeeItem(TransactionStatus.ROLLED_BACK);
    }

    private void whenDeletedItemIsPersistedButNotCommittedThenUserSeesItem(TransactionStatus txStatusOverride) {
        createItemWithUnlockingCommit(drEvilSavingsAccount);
        QueryRequest queryRequest = getDrEvilSavingsQueryRequest();
        QueryResult queryResult = coordinator.query(queryRequest);
        assertEquals(1, queryResult.getItems().size());
        String transactionId = randomTransactionId();
        coordinator.startTransaction(transactionId);
        coordinator.deleteItem(getDrEvilSavingsDeleteItemRequest());
        coordinator.commitWithoutUnlocking();
        queryRequest = getDrEvilSavingsQueryRequest();
        queryResult = coordinator.query(queryRequest);
        assertEquals(0, queryResult.getItems().size());
        overrideTransactionStatus(transactionId, txStatusOverride);
        queryRequest = getDrEvilSavingsQueryRequest();
        queryResult = coordinator.query(queryRequest);
        assertEquals(1, queryResult.getItems().size());
    }

    @Test
    public void whenDeletedItemIsPersistedAndParentTransactionIsStartedThenUserSeesItem() {
        whenDeletedItemIsPersistedButNotCommittedThenUserSeesItem(TransactionStatus.START_COMMIT);
    }

    @Test
    public void whenDeletedItemIsPersistedAndParentTransactionIsRolledBackThenUserSeesItem() {
        whenDeletedItemIsPersistedButNotCommittedThenUserSeesItem(TransactionStatus.ROLLED_BACK);
    }

    @Test
    public void userSuppliedFilterExpressionNarrowsResults() {
        createItemWithUnlockingCommit(drEvilSavingsAccount);
        QueryRequest request = getDrEvilSavingsQueryRequest();
        request.getExpressionAttributeNames().put("#amount", BankAccountItemMapper.TOTAL_AMOUNT_IN_USD_KEY_NAME);
        request.getExpressionAttributeValues().put(":amount", new AttributeValue().withN(drEvilSavingsAccount.getTotalAmountInUsd().toString()));
        request.setFilterExpression("#amount > :amount");
        QueryResult result = coordinator.query(request);
        assertTrue(result.getItems().isEmpty());
    }

    @Test
    public void userCanPaginateQuery() {
        BankAccountItem drEvilCheckingAccount = new BankAccountItem(drEvilSavingsAccount.getBeneficiaryName(), "checking", 1000);
        createItemWithUnlockingCommit(drEvilCheckingAccount);
        createItemWithUnlockingCommit(drEvilSavingsAccount);
        QueryRequest request = getDrEvilSavingsQueryRequest();
        request.getExpressionAttributeValues().put(":rangevalue", new AttributeValue("xyz")); // "xyz" is greater than "checking" and "savings"
        request.setKeyConditionExpression("#hashkey = :hashvalue AND #rangekey < :rangevalue");
        request.setLimit(1);
        QueryResult result = coordinator.query(request);
        assertEquals(1, result.getItems().size());
        assertNotNull(result.getLastEvaluatedKey());
        BankAccountItem firstAccount = bankAccountItemMapper.unmarshall(result.getItems().get(0));
        request.setExclusiveStartKey(result.getLastEvaluatedKey());
        result = coordinator.query(request);
        assertEquals(1, result.getItems().size());
        assertNotNull(result.getLastEvaluatedKey());
        BankAccountItem secondAccount = bankAccountItemMapper.unmarshall(result.getItems().get(0));
        assertTrue(firstAccount.getAccountType() != secondAccount.getAccountType());
        request.setExclusiveStartKey(result.getLastEvaluatedKey());
        result = coordinator.query(request);
        assertEquals(0, result.getItems().size());
        assertNull(result.getLastEvaluatedKey());
    }

    @Test
    public void userCanQueryLSI() {
        createItemWithoutUnlockingCommit(drEvilSavingsAccount);
        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#hashkey", bankAccountItemMapper.getHashKeyName());
        expressionAttributeNames.put("#rangekey", BankAccountItemMapper.TOTAL_AMOUNT_IN_USD_KEY_NAME);
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":hashvalue", new AttributeValue(drEvilSavingsAccount.getBeneficiaryName()));
        expressionAttributeValues.put(":rangevalue", new AttributeValue().withN(drEvilSavingsAccount.getTotalAmountInUsd().toString()));
        QueryRequest queryRequest = new QueryRequest().
                withConsistentRead(true).
                withTableName(bankAccountItemMapper.getTableName()).
                withIndexName(BankAccountItemMapper.TOTAL_AMOUNT_LSI_NAME).
                withExpressionAttributeNames(expressionAttributeNames).
                withExpressionAttributeValues(expressionAttributeValues).
                withKeyConditionExpression("#hashkey = :hashvalue AND #rangekey = :rangevalue");
        QueryResult result = coordinator.query(queryRequest);
        List<Map<String, AttributeValue>> items = result.getItems();
        assertEquals(1, items.size());
        BankAccountItem returnedItem = bankAccountItemMapper.unmarshall(items.get(0));
        assertEquals(drEvilSavingsAccount.getBeneficiaryName(), returnedItem.getBeneficiaryName());
        assertEquals(drEvilSavingsAccount.getAccountType(), returnedItem.getAccountType());
        assertEquals(drEvilSavingsAccount.getTotalAmountInUsd(), returnedItem.getTotalAmountInUsd());
    }

    @Test
    public void synchronousRollbackUnlocksAddedItem() {
        String transactionId = randomTransactionId();
        coordinator.startTransaction(transactionId);
        coordinator.createItem(bankAccountItemMapper.generateUpdateItemRequest(drEvilSavingsAccount));
        coordinator.commitWithoutUnlocking();
        QueryRequest queryRequest = getDrEvilSavingsQueryRequest();
        QueryResult queryResult = coordinator.query(queryRequest);
        assertEquals(1, queryResult.getItems().size());
        overrideTransactionStatus(transactionId, TransactionStatus.START_COMMIT);
        coordinator.reloadTransactionLogItem();
        List<Future> unlockFutures = coordinator.rollback();
        Futures.blockOnAllFutures(unlockFutures, Instant.now().plusSeconds(1));
        queryResult = coordinator.query(queryRequest);
        assertEquals(0, queryResult.getItems().size());
        Map<String, AttributeValue> rawTxLogItem = getRawTxLogItem(transactionId);
        assertEquals(TransactionStatus.ROLLED_BACK.name(), rawTxLogItem.get(TransactionLogItemMapper.TRANSACTION_STATUS_KEY_NAME).getS());
    }

    @Test(expected = ContentionException.class)
    public void whenTransactionRecordIsSweepedRollbackThrowsContentionException() {
        String transactionId = randomTransactionId();
        coordinator.startTransaction(transactionId);
        coordinator.createItem(bankAccountItemMapper.generateUpdateItemRequest(drEvilSavingsAccount));
        coordinator.commitWithoutUnlocking();

        overrideTransactionStatus(transactionId, TransactionStatus.START_COMMIT);
        coordinator.reloadTransactionLogItem();
        deleteTransactionLogItem(transactionId);
        coordinator.rollback();
    }

    @Test
    public void synchronousRollbackUnlocksDeletedItem() {
        createItemWithUnlockingCommit(drEvilSavingsAccount);
        String transactionId = randomTransactionId();
        coordinator.startTransaction(transactionId);
        coordinator.deleteItem(getDrEvilSavingsDeleteItemRequest());
        coordinator.commitWithoutUnlocking();
        QueryRequest queryRequest = getDrEvilSavingsQueryRequest();
        QueryResult queryResult = coordinator.query(queryRequest);
        assertEquals(0, queryResult.getItems().size());
        overrideTransactionStatus(transactionId, TransactionStatus.START_COMMIT);
        coordinator.reloadTransactionLogItem();
        List<Future> unlockFutures = coordinator.rollback();
        Futures.blockOnAllFutures(unlockFutures, Instant.now().plusSeconds(1));
        queryResult = coordinator.query(queryRequest);
        assertEquals(1, queryResult.getItems().size());
        Map<String, AttributeValue> rawTxLogItem = getRawTxLogItem(transactionId);
        assertEquals(TransactionStatus.ROLLED_BACK.name(), rawTxLogItem.get(TransactionLogItemMapper.TRANSACTION_STATUS_KEY_NAME).getS());
    }

    @Test
    public void exceptionsDuringPostCommitUnlockAreNotThrown() {
        createItemWithUnlockingCommit(drEvilSavingsAccount);
        coordinator.executePostCommitUnlocks(); // Sync commit already unlocked
    }

    @Test
    public void scanDoesNotExposeUncommittedAddedItem() {
        String transactionId = randomTransactionId();
        createItemWithoutUnlockingCommit(transactionId, drEvilSavingsAccount);
        overrideTransactionStatus(transactionId, TransactionStatus.START_COMMIT);
        ScanResult result = coordinator.scan(bankAccountItemMapper.generateScanRequest());
        assertTrue(result.getItems().isEmpty());
        assertNull(result.getLastEvaluatedKey());
    }

    @Test
    public void scanExposesUncommittedDeletedItem() {
        String transactionId = randomTransactionId();
        createItemWithUnlockingCommit(drEvilSavingsAccount);
        coordinator.startTransaction(transactionId);
        coordinator.deleteItem(getDrEvilSavingsDeleteItemRequest());
        coordinator.commitWithoutUnlocking();
        overrideTransactionStatus(transactionId, TransactionStatus.START_COMMIT);
        ScanResult result = coordinator.scan(bankAccountItemMapper.generateScanRequest());
        assertEquals(1, result.getItems().size());
        assertNull(result.getLastEvaluatedKey());
    }

    @Test
    public void scanExposesCommittedItems() {
        createItemWithUnlockingCommit(drEvilSavingsAccount);
        ScanResult result = coordinator.scan(bankAccountItemMapper.generateScanRequest());
        assertEquals(1, result.getItems().size());
        assertNull(result.getLastEvaluatedKey());
    }

    @Test
    public void scanIsolatesCommittedButLockedAddedAndDeletedItems() {
        createItemWithUnlockingCommit(drEvilSavingsAccount);
        coordinator.startTransaction();
        coordinator.deleteItem(getDrEvilSavingsDeleteItemRequest());
        coordinator.commitWithoutUnlocking();
        BankAccountItem drEvilCheckingAccount = new BankAccountItem(drEvilSavingsAccount.getBeneficiaryName(), "checking", 1000);
        createItemWithoutUnlockingCommit(drEvilCheckingAccount);
        ScanResult result = coordinator.scan(bankAccountItemMapper.generateScanRequest());
        assertEquals(1, result.getItems().size());
        assertTrue("checking".equals(bankAccountItemMapper.unmarshall(result.getItems().get(0)).getAccountType()));
        assertNull(result.getLastEvaluatedKey());
    }

    @Test
    public void userSpecifiedPageSizeLimitIsExercisedOnScan() {
        BankAccountItem drEvilCheckingAccount = new BankAccountItem(drEvilSavingsAccount.getBeneficiaryName(), "checking", 1000);
        createItemWithUnlockingCommit(drEvilSavingsAccount);
        createItemWithUnlockingCommit(drEvilCheckingAccount);
        ScanRequest scanRequest = bankAccountItemMapper.generateScanRequest();
        scanRequest.withLimit(1);
        ScanResult result = coordinator.scan(scanRequest);
        assertEquals(1, result.getItems().size());
        assertNotNull(result.getLastEvaluatedKey());
        scanRequest.withExclusiveStartKey(result.getLastEvaluatedKey());
        result = coordinator.scan(scanRequest);
        assertEquals(1, result.getItems().size());
        if (result.getLastEvaluatedKey() != null) {
            scanRequest.withExclusiveStartKey(result.getLastEvaluatedKey());
            result = coordinator.scan(scanRequest);
            assertTrue(result.getItems().isEmpty());
            assertNull(result.getLastEvaluatedKey());
        }
    }

    @Test
    public void userSpecifiedScanFiltersAreApplied() {
        createItemWithUnlockingCommit(drEvilSavingsAccount);
        ScanRequest scanRequest = bankAccountItemMapper.generateScanRequest();
        scanRequest.setExpressionAttributeNames(Collections.singletonMap("#amount", BankAccountItemMapper.TOTAL_AMOUNT_IN_USD_KEY_NAME));
        scanRequest.setExpressionAttributeValues(Collections.singletonMap(":amount", new AttributeValue().withN(drEvilSavingsAccount.getTotalAmountInUsd().toString())));
        scanRequest.setFilterExpression("#amount > :amount");
        ScanResult result = coordinator.scan(scanRequest);
        assertTrue(result.getItems().isEmpty());
        assertNull(result.getLastEvaluatedKey());
    }

    @Test
    public void scanResultsDontHaveControlFields() {
        createItemWithoutUnlockingCommit(drEvilSavingsAccount);
        ScanResult result = coordinator.scan(bankAccountItemMapper.generateScanRequest());
        assertEquals(1, result.getItems().size());
        assertNull(result.getLastEvaluatedKey());
        Map<String, AttributeValue> rawItem = result.getItems().get(0);
        assertFalse(rawItem.containsKey(TransactionCoordinator.TRANSACTION_ID_CONTROL_FIELD));
        assertFalse(rawItem.containsKey(TransactionCoordinator.TRANSACTION_OPERATION_CONTROL_FIELD));
    }
}
