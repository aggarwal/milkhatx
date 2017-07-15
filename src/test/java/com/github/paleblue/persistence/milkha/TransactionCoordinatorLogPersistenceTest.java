package com.github.paleblue.persistence.milkha;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.github.paleblue.persistence.milkha.dto.TransactionLogItem;
import com.github.paleblue.persistence.milkha.dto.TransactionStatus;
import com.github.paleblue.persistence.milkha.exception.ContentionException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;

public class TransactionCoordinatorLogPersistenceTest extends TransactionCoordinatorBaseTest {

    private String transactionId;

    @Before
    public void setup() {
        transactionId = randomTransactionId();
    }

    @Test
    public void transactionLogItemIsPersistedForEmptyCommittedTransaction() {
        coordinator.startTransaction(transactionId);
        coordinator.commit();
        getAndAssertTransactionLogItem(transactionId, TransactionStatus.COMMITTED);
    }

    @Test
    public void transactionLogItemIsPersistedForEmptyAsyncCommittedTransaction() {
        coordinator.startTransaction(transactionId);
        coordinator.commitWithoutUnlocking();
        getAndAssertTransactionLogItem(transactionId, TransactionStatus.COMMITTED);
    }

    @Test
    public void transactionLogItemIsNotPersistedForEmptyRolledBackTransaction() {
        coordinator.startTransaction(transactionId);
        coordinator.rollback();
        assertNull(getTransactionLogItem(transactionId));
    }

    @Test
    public void transactionLogItemIsNotPersistedForEmptyAsyncRolledBackTransaction() {
        coordinator.startTransaction(transactionId);
        coordinator.rollbackWithoutUnlocking();
        assertNull(getTransactionLogItem(transactionId));
    }

    @Test(expected = ContentionException.class)
    public void whenDuplicateTransactionIdIsSuppliedThenTransactionFails() {
        coordinator.startTransaction(transactionId);
        coordinator.commit();
        coordinator.startTransaction(transactionId);
        coordinator.commit();
    }

    private void getAndAssertTransactionLogItem(String transactionId, TransactionStatus expectedTransactionStatus) {
        TransactionLogItem txLogItem = getTransactionLogItem(transactionId);
        assertNotNull(txLogItem);
        assertEquals(transactionId, txLogItem.getTransactionId());
        assertEquals(expectedTransactionStatus, txLogItem.getTransactionStatus());
        assertTrue(txLogItem.getCreateSet().isEmpty());
        assertTrue(txLogItem.getDeleteSet().isEmpty());
    }

    private TransactionLogItem getTransactionLogItem(String transactionId) {
        GetItemRequest request = txLogItemMapper.generateGetItemRequest(transactionId);
        GetItemResult result = ddbClient.getItem(request);
        Map<String, AttributeValue> txLogItemMap = result.getItem();
        if (txLogItemMap != null) {
            return txLogItemMapper.unmarshall(txLogItemMap);
        } else {
            return null;
        }
    }
}
