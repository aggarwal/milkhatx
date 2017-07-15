package com.github.paleblue.persistence.milkha.dto;


import java.util.UUID;

import com.github.paleblue.persistence.milkha.exception.InvalidTransactionStateTransitionException;
import org.junit.Test;

public class TransactionLogItemTest {

    private static final long WAIT_PERIOD_BEFORE_SWEEPER_UNLOCK_MILLIS = 3000L;
    private static final long WAIT_PERIOD_BEFORE_SWEEPER_DELETE_MILLIS = 6000L;

    @Test(expected = InvalidTransactionStateTransitionException.class)
    public void setTransactionStatusValidatesTransitions() {
        String transactionId = UUID.randomUUID().toString();
        TransactionLogItem txLogItem = new TransactionLogItem(transactionId, TransactionStatus.START_COMMIT, WAIT_PERIOD_BEFORE_SWEEPER_UNLOCK_MILLIS,
                WAIT_PERIOD_BEFORE_SWEEPER_DELETE_MILLIS);
        txLogItem.setTransactionStatus(TransactionStatus.NOT_PERSISTED);
    }

}
