package com.github.paleblue.persistence.milkha.dto;


import com.github.paleblue.persistence.milkha.exception.InvalidTransactionStateTransitionException;
import org.junit.Test;

public class TransactionStatusTest {

    @Test
    public void validateTransitionDoesNotThrowOnValidTransitions() {
        TransactionStatus.validateStateTransition(TransactionStatus.NOT_PERSISTED, TransactionStatus.START_COMMIT);
        TransactionStatus.validateStateTransition(TransactionStatus.START_COMMIT, TransactionStatus.COMMITTED);
        TransactionStatus.validateStateTransition(TransactionStatus.START_COMMIT, TransactionStatus.ROLLED_BACK);
    }

    @Test(expected = InvalidTransactionStateTransitionException.class)
    public void validateTransitionThrowsOnInvalidTransition() {
        TransactionStatus.validateStateTransition(TransactionStatus.START_COMMIT, TransactionStatus.NOT_PERSISTED);
    }

    @Test(expected = InvalidTransactionStateTransitionException.class)
    public void validateTransitionThrowsWhenFromIsNull() {
        TransactionStatus.validateStateTransition(null, TransactionStatus.NOT_PERSISTED);
    }
}
