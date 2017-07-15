package com.github.paleblue.persistence.milkha.mapper;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Map;
import java.util.UUID;

import com.github.paleblue.persistence.milkha.dto.TransactionLogItem;
import com.github.paleblue.persistence.milkha.dto.TransactionStatus;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.junit.Before;
import org.junit.Test;

public class TransactionLogItemMapperTest {
    private static final long WAIT_PERIOD_BEFORE_SWEEPER_UNLOCK_MILLIS = 3000L;
    private static final long WAIT_PERIOD_BEFORE_SWEEPER_DELETE_MILLIS = 6000L;

    TransactionLogItemMapper marshaller;

    @Before
    public void setup() {
        marshaller = new TransactionLogItemMapper();
    }

    @Test
    public void marshallsAllFields() {
        String transactionId = UUID.randomUUID().toString();
        TransactionLogItem txLogItem = new TransactionLogItem(transactionId, TransactionStatus.NOT_PERSISTED, WAIT_PERIOD_BEFORE_SWEEPER_UNLOCK_MILLIS, WAIT_PERIOD_BEFORE_SWEEPER_DELETE_MILLIS);
        Map<String, AttributeValue> map = marshaller.marshall(txLogItem);
        assertEquals(transactionId, map.get(TransactionLogItemMapper.TRANSACTION_ID_KEY_NAME).getS());
        assertEquals(TransactionStatus.NOT_PERSISTED.name(), map.get(TransactionLogItemMapper.TRANSACTION_STATUS_KEY_NAME).getS());
        assertEquals(String.valueOf(WAIT_PERIOD_BEFORE_SWEEPER_UNLOCK_MILLIS), map.get(TransactionLogItemMapper.WAIT_PERIOD_BEFORE_SWEEPER_UNLOCK_MILLIS).getN());
        assertEquals(String.valueOf(WAIT_PERIOD_BEFORE_SWEEPER_DELETE_MILLIS), map.get(TransactionLogItemMapper.WAIT_PERIOD_BEFORE_SWEEPER_DELETE_MILLIS).getN());
        assertFalse(map.get(TransactionLogItemMapper.UNLOCKED_BY_SWEEPER).getBOOL());
    }

    @Test
    public void marshallUnmarshallYieldsEqualObject() {
        String transactionId = UUID.randomUUID().toString();
        TransactionLogItem originalTxLogItem = new TransactionLogItem(transactionId, TransactionStatus.NOT_PERSISTED, WAIT_PERIOD_BEFORE_SWEEPER_UNLOCK_MILLIS,
                WAIT_PERIOD_BEFORE_SWEEPER_DELETE_MILLIS);
        Map<String, AttributeValue> itemAttributeValueMap = marshaller.marshall(originalTxLogItem);
        TransactionLogItem unmarshalledTxLogItem = marshaller.unmarshall(itemAttributeValueMap);
        assertEquals(originalTxLogItem.getTransactionId(), unmarshalledTxLogItem.getTransactionId());
        assertEquals(originalTxLogItem.getTransactionStatus(), unmarshalledTxLogItem.getTransactionStatus());
        assertEquals(originalTxLogItem.getWaitPeriodBeforeSweeperUnlockMillis(), unmarshalledTxLogItem.getWaitPeriodBeforeSweeperUnlockMillis());
        assertEquals(originalTxLogItem.getWaitPeriodBeforeSweeperDeleteMillis(), unmarshalledTxLogItem.getWaitPeriodBeforeSweeperDeleteMillis());
        assertEquals(originalTxLogItem.isUnlockedBySweeper(), unmarshalledTxLogItem.isUnlockedBySweeper());
    }
}
