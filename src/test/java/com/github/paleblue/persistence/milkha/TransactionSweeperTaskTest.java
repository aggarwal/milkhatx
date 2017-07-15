package com.github.paleblue.persistence.milkha;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.Test;

import com.github.paleblue.persistence.milkha.dto.BankAccountItem;
import com.github.paleblue.persistence.milkha.dto.TransactionLogItem;
import com.github.paleblue.persistence.milkha.dto.TransactionStatus;
import com.github.paleblue.persistence.milkha.exception.UnexpectedTransactionStateException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

public class TransactionSweeperTaskTest extends TransactionCoordinatorBaseTest {

    private String transactionId;
    private TransactionSweeperTask txSweeperTask;
    private BankAccountItem drEvilSavingsAccount;
    private BankAccountItem drEvilCheckingAccount;

    @Before
    public void setup() {
        transactionId = UUID.randomUUID().toString();
        String beneficiaryName = "DrEvil-" + UUID.randomUUID().toString();
        drEvilSavingsAccount = new BankAccountItem(beneficiaryName, SAVINGS_ACCOUNT_TYPE, 1000000);
        drEvilCheckingAccount = new BankAccountItem(beneficiaryName, CHECKING_ACCOUNT_TYPE, 1000);
    }

    private void assertItemIsStillLocked(BankAccountItem bankAccountItem) {
        Map<String, AttributeValue> rawBankAccountItem = getRawBankAccountItem(bankAccountItem);
        assertTrue(rawBankAccountItem.containsKey(TransactionCoordinator.TRANSACTION_ID_CONTROL_FIELD));
        assertTrue(rawBankAccountItem.containsKey(TransactionCoordinator.TRANSACTION_OPERATION_CONTROL_FIELD));
    }

    @Test
    public void sweeperDoesNotUnlockOrSweepOrDeleteIfTheUnlockPeriodHasNotPassed() throws ExecutionException, InterruptedException {
        createItemWithoutUnlockingCommit(transactionId, drEvilSavingsAccount);
        txSweeperTask = new TransactionSweeperTask(getTxLogItem(transactionId), Instant.now().minusMillis(1), ddbClient, txLogItemMapper);
        assertFalse(txSweeperTask.execute());
        assertItemIsStillLocked(drEvilSavingsAccount);
        Map<String, AttributeValue> rawTxLogItem = getRawTxLogItem(transactionId);
        assertNotNull(rawTxLogItem); // log item was not deleted by sweeper
        TransactionLogItem txLogItem = txLogItemMapper.unmarshall(rawTxLogItem);
        assertFalse(txLogItem.isUnlockedBySweeper());
    }

    @Test
    public void sweeperDoesNotExecuteCommitAndDeleteSetOperationsIfLogItemIsMarkedAsUnlocked() throws InterruptedException, ExecutionException {
        createItemWithoutUnlockingCommit(transactionId, drEvilSavingsAccount);
        TransactionLogItem txLogItem = getTxLogItem(transactionId);
        txLogItem.setWaitPeriodBeforeSweeperUnlockMillis(0L);
        txLogItem.setWaitPeriodBeforeSweeperDeleteMillis(3000L);
        overrideSweeperUnlockStatus(transactionId, true);
        txSweeperTask = new TransactionSweeperTask(txLogItem, Instant.now().minusMillis(1), ddbClient, txLogItemMapper); assertFalse(txSweeperTask.execute());
        assertItemIsStillLocked(drEvilSavingsAccount);
        txLogItem = getTxLogItem(transactionId);
        assertNotNull(txLogItem);
        assertTrue(txLogItem.isUnlockedBySweeper());
    }


    @Test
    public void sweeperDeletesPreviouslyUnlockedTransaction() throws ExecutionException, InterruptedException {
        createItemWithoutUnlockingCommit(transactionId, drEvilSavingsAccount);
        TransactionLogItem txLogItem = getTxLogItem(transactionId);
        txLogItem.setWaitPeriodBeforeSweeperUnlockMillis(0L);
        txLogItem.setWaitPeriodBeforeSweeperDeleteMillis(0L);
        overrideSweeperUnlockStatus(transactionId, true);
        txSweeperTask = new TransactionSweeperTask(txLogItem, Instant.now().minusMillis(1), ddbClient, txLogItemMapper);
        assertTrue(txSweeperTask.execute());
        assertNull(getRawTxLogItem(transactionId));
    }


    @Test
    public void sweeperUnlocksCommittedItems() throws ExecutionException, InterruptedException {
        createItemWithUnlockingCommit(drEvilCheckingAccount);
        createAndDeleteItemWithoutUnlockingCommit(transactionId, drEvilSavingsAccount, drEvilCheckingAccount);
        TransactionLogItem txLogItem = getTxLogItem(transactionId);
        txLogItem.setWaitPeriodBeforeSweeperUnlockMillis(0L);
        txLogItem.setWaitPeriodBeforeSweeperDeleteMillis(20000L);
        txSweeperTask = new TransactionSweeperTask(txLogItem, Instant.now().minusMillis(1), ddbClient, txLogItemMapper);
        assertFalse(txSweeperTask.execute());
        Map<String, AttributeValue> rawBankAccountItem = getRawBankAccountItem(drEvilSavingsAccount);
        assertFalse(rawBankAccountItem.containsKey(TransactionCoordinator.TRANSACTION_ID_CONTROL_FIELD));
        assertFalse(rawBankAccountItem.containsKey(TransactionCoordinator.TRANSACTION_OPERATION_CONTROL_FIELD));
        assertNull(getRawBankAccountItem(drEvilCheckingAccount));
        txLogItem = getTxLogItem(transactionId);
        assertTrue(txLogItem.isUnlockedBySweeper());
    }

    @Test
    public void sweeperUnlocksRolledBackItems() throws ExecutionException, InterruptedException {
        createItemWithUnlockingCommit(drEvilCheckingAccount);
        createAndDeleteItemWithoutUnlockingCommit(transactionId, drEvilSavingsAccount, drEvilCheckingAccount);
        TransactionLogItem txLogItem = getTxLogItem(transactionId);
        txLogItem.setWaitPeriodBeforeSweeperUnlockMillis(0L);
        txLogItem.setWaitPeriodBeforeSweeperDeleteMillis(20000L);
        overrideTransactionStatus(transactionId, TransactionStatus.ROLLED_BACK);
        assertNotNull(getRawBankAccountItem(drEvilSavingsAccount));
        txSweeperTask = new TransactionSweeperTask(txLogItem, Instant.now().minusMillis(1), ddbClient, txLogItemMapper);
        assertFalse(txSweeperTask.execute());
        assertNull(getRawBankAccountItem(drEvilSavingsAccount));
        assertNotNull(getRawBankAccountItem(drEvilCheckingAccount));
    }

    @Test
    public void sweeperUnlocksPartiallyUnlockedCommittedTransaction() throws ExecutionException, InterruptedException {
        createItemWithUnlockingCommit(drEvilCheckingAccount);
        createAndDeleteItemWithoutUnlockingCommit(transactionId, drEvilSavingsAccount, drEvilCheckingAccount);
        TransactionLogItem txLogItem = getTxLogItem(transactionId);
        txLogItem.setWaitPeriodBeforeSweeperUnlockMillis(0L);
        txLogItem.setWaitPeriodBeforeSweeperDeleteMillis(20000L);
        ddbClient.putItem(bankAccountItemMapper.generatePutItemRequest(drEvilSavingsAccount)); // simulate unlock
        txSweeperTask = new TransactionSweeperTask(txLogItem, Instant.now().minusMillis(1), ddbClient, txLogItemMapper);
        assertFalse(txSweeperTask.execute());
        assertNull(getRawBankAccountItem(drEvilCheckingAccount));
        txLogItem = getTxLogItem(transactionId);
        assertTrue(txLogItem.isUnlockedBySweeper());
    }

    @Test
    public void sweeperUnlocksPartiallyUnlockedRolledBackTransaction() throws ExecutionException, InterruptedException {
        createItemWithUnlockingCommit(drEvilCheckingAccount);
        createAndDeleteItemWithoutUnlockingCommit(transactionId, drEvilSavingsAccount, drEvilCheckingAccount);
        TransactionLogItem txLogItem = getTxLogItem(transactionId);
        txLogItem.setWaitPeriodBeforeSweeperUnlockMillis(0L);
        txLogItem.setWaitPeriodBeforeSweeperDeleteMillis(20000L);
        overrideTransactionStatus(transactionId, TransactionStatus.ROLLED_BACK);
        ddbClient.deleteItem(bankAccountItemMapper.generateDeleteItemRequest(drEvilSavingsAccount.getBeneficiaryName(), drEvilSavingsAccount.getAccountType()));
        txSweeperTask = new TransactionSweeperTask(txLogItem, Instant.now().minusMillis(1), ddbClient, txLogItemMapper);
        assertFalse(txSweeperTask.execute());
        assertFalse(getRawBankAccountItem(drEvilCheckingAccount).containsKey(TransactionCoordinator.TRANSACTION_ID_CONTROL_FIELD));
        txLogItem = getTxLogItem(transactionId);
        assertTrue(txLogItem.isUnlockedBySweeper());
    }

    @Test
    public void sweeperRollsBackAndUnlocksUncommittedTransactionThatHasTimedOut() throws ExecutionException, InterruptedException {
        createItemWithUnlockingCommit(drEvilCheckingAccount);
        createAndDeleteItemWithoutUnlockingCommit(transactionId, drEvilSavingsAccount, drEvilCheckingAccount);
        TransactionLogItem txLogItem = getTxLogItem(transactionId);
        txLogItem.setWaitPeriodBeforeSweeperUnlockMillis(0L);
        txLogItem.setWaitPeriodBeforeSweeperDeleteMillis(20000L);
        overrideTransactionStatus(transactionId, TransactionStatus.START_COMMIT);
        txSweeperTask = new TransactionSweeperTask(txLogItem, Instant.now().minusMillis(1), ddbClient, txLogItemMapper);
        assertFalse(txSweeperTask.execute());
        assertNull(getRawBankAccountItem(drEvilSavingsAccount));
        assertFalse(getRawBankAccountItem(drEvilCheckingAccount).containsKey(TransactionCoordinator.TRANSACTION_ID_CONTROL_FIELD));;
        txLogItem = getTxLogItem(transactionId);
        assertTrue(txLogItem.isUnlockedBySweeper());
        assertEquals(TransactionStatus.ROLLED_BACK, txLogItem.getTransactionStatus());
    }

    @Test
    public void sweeperDeletesTransactionLogItemAfterUnlocking() throws ExecutionException, InterruptedException {
        createItemWithoutUnlockingCommit(transactionId, drEvilSavingsAccount);
        TransactionLogItem txLogItem = getTxLogItem(transactionId);
        txLogItem.setWaitPeriodBeforeSweeperUnlockMillis(0L);
        txLogItem.setWaitPeriodBeforeSweeperDeleteMillis(0L);
        txSweeperTask = new TransactionSweeperTask(txLogItem, Instant.now().minusMillis(1), ddbClient, txLogItemMapper);
        assertTrue(txSweeperTask.execute());
        assertFalse(getRawBankAccountItem(drEvilSavingsAccount).containsKey(TransactionCoordinator.TRANSACTION_ID_CONTROL_FIELD));
        assertNull(getRawTxLogItem(transactionId));
    }

    @Test(expected = UnexpectedTransactionStateException.class)
    public void sweeperThrowsExceptionWhenTransactionStatusIsUnexpected() throws Throwable {
        createItemWithoutUnlockingCommit(transactionId, drEvilSavingsAccount);
        overrideTransactionStatus(transactionId, TransactionStatus.COMPLETE);
        TransactionLogItem txLogItem = getTxLogItem(transactionId);
        txLogItem.setWaitPeriodBeforeSweeperUnlockMillis(0L);
        txLogItem.setWaitPeriodBeforeSweeperDeleteMillis(20000L);
        txSweeperTask = new TransactionSweeperTask(txLogItem, Instant.now().minusMillis(1), ddbClient, txLogItemMapper);
        txSweeperTask.execute();
    }


    @Test
    public void doNotThrowExceptionIfTransactionLogItemDoesNotExist() {
        TransactionLogItem txLogItem = new TransactionLogItem(transactionId, TransactionStatus.START_COMMIT, 0L, 0L);
        txSweeperTask = new TransactionSweeperTask(txLogItem, Instant.now().minusMillis(1), ddbClient, txLogItemMapper);
        assertTrue(txSweeperTask.execute());
    }

    @Test
    public void createdItemCanBeDeletedAfterSweeperUnlocksIt() throws ExecutionException, InterruptedException {
        createItemWithoutUnlockingCommit(transactionId, drEvilSavingsAccount);
        TransactionLogItem txLogItem = getTxLogItem(transactionId);
        txLogItem.setWaitPeriodBeforeSweeperUnlockMillis(0L);
        txLogItem.setWaitPeriodBeforeSweeperDeleteMillis(20000L);
        txSweeperTask = new TransactionSweeperTask(txLogItem, Instant.now().minusMillis(1), ddbClient, txLogItemMapper);
        assertFalse(txSweeperTask.execute());
        assertNotNull(getRawBankAccountItem(drEvilSavingsAccount));
        coordinator.startTransaction();
        coordinator.deleteItem(bankAccountItemMapper.generateDeleteItemRequest(drEvilSavingsAccount.getBeneficiaryName(),
                drEvilSavingsAccount.getAccountType()));
        coordinator.commit().get(0).get();
        assertNull(getRawBankAccountItem(drEvilSavingsAccount));
    }
}
