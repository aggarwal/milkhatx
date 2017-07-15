package com.github.paleblue.persistence.milkha;


import org.junit.Test;

import com.github.paleblue.persistence.milkha.exception.TransactionNotStartedException;
import com.github.paleblue.persistence.milkha.exception.TransactionPendingException;

public class TransactionCoordinatorTest extends TransactionCoordinatorBaseTest {


    @Test(expected = IllegalArgumentException.class)
    public void whenSpecifiedNegativeTimeoutThenTxCoordinatorFactoryThrowsIllegalArgumentException() {
        coordinatorBuilder.withMaxTimeToCommitOrRollbackMillis(-12345L).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenSpecifiedZeroTimeoutThenTxCoordinatorFactoryThrowsIllegalArgumentException() {
        coordinatorBuilder.withMaxTimeToCommitOrRollbackMillis(0L).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenSpecifiedAnUnlockWaitPeriodThatIsLessThanTimeoutThrowsIllegalArgumentException() {
        coordinatorBuilder.withWaitPeriodBeforeSweeperUnlockMillis(900L).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenSpecifiedADeleteWaitPeriodThatIsLessThanUnlockWaitPeriodThrowsIllegalArgumentException() {
        coordinatorBuilder.withWaitPeriodBeforeSweeperDeleteMillis(2000L).build();
    }

    @Test
    public void whenSpecifiedValidArgumentsThenNoExceptionIsThrown() {
        coordinatorBuilder.build();
    }

    @Test(expected = TransactionPendingException.class)
    public void coordinatorThrowsExceptionIfTransactionPending() {
        coordinator.startTransaction();
        coordinator.startTransaction();
    }

    @Test
    public void coordinatorStartsNewTransactionIfPreviousCommitted() {
        coordinator.startTransaction();
        coordinator.commit();
        coordinator.startTransaction();
    }

    @Test
    public void coordinatorStartsNewTransactionIfPreviousAsyncCommitted() {
        coordinator.startTransaction();
        coordinator.commitWithoutUnlocking();
        coordinator.startTransaction();
    }

    @Test
    public void coordinatorStartsNewTransactionIfPreviousRolledBack() {
        coordinator.startTransaction();
        coordinator.rollback();
        coordinator.startTransaction();
    }

    @Test
    public void coordinatorStartsNewTransactionIfPreviousAsyncRolledBack() {
        coordinator.startTransaction();
        coordinator.rollbackWithoutUnlocking();
        coordinator.startTransaction();
    }

    @Test(expected = TransactionNotStartedException.class)
    public void whenCommitIsInvokedWithoutStartingTransactionThenThrowException() {
        coordinator.commit();
    }

    @Test(expected = TransactionNotStartedException.class)
    public void whenCommitAsyncIsInvokedWithoutStartingTransactionThenThrowException() {
        coordinator.commitWithoutUnlocking();
    }

    @Test(expected = TransactionNotStartedException.class)
    public void whenRollbackIsInvokedWithoutStartingTransactionThenThrowException() {
        coordinator.rollback();
    }

    @Test(expected = TransactionNotStartedException.class)
    public void whenRollbackAsyncIsInvokedWithoutStartingTransactionThenThrowException() {
        coordinator.rollbackWithoutUnlocking();
    }

}
