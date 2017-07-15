package com.github.paleblue.persistence.milkha;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import com.github.paleblue.persistence.milkha.dto.BankAccountItem;
import com.github.paleblue.persistence.milkha.dto.TransactionLogItem;
import com.github.paleblue.persistence.milkha.dto.TransactionStatus;
import com.github.paleblue.persistence.milkha.mapper.BankAccountItemMapper;
import com.github.paleblue.persistence.milkha.mapper.TransactionLogItemMapper;
import com.github.paleblue.persistence.milkha.util.Futures;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;

@Ignore
public class TransactionCoordinatorBaseTest extends DynamoDBLocalAbstractTest {

    protected static final String SAVINGS_ACCOUNT_TYPE = "savings";
    protected static final String CHECKING_ACCOUNT_TYPE = "checking";
    protected static final long MAX_TIME_TO_COMMIT_OR_ROLLBACK_MILLIS = 2000L;
    protected static final long WAIT_PERIOD_BEFORE_SWEEPER_UNLOCK_MILLIS = 3000L;
    protected static final long WAIT_PERIOD_BEFORE_SWEEPER_DELETE_MILLIS = 6000L;
    
    protected TransactionCoordinator coordinator;
    protected TransactionCoordinatorBuilder coordinatorBuilder;
    protected BankAccountItemMapper bankAccountItemMapper;
    protected TransactionLogItemMapper txLogItemMapper;
    protected Random random;

    public TransactionCoordinatorBaseTest() {
        coordinatorBuilder = new TransactionCoordinatorBuilder(ddbClient, Executors.newSingleThreadExecutor());
        random = new Random();
    }

    @Before
    public void init() {
        bankAccountItemMapper = new BankAccountItemMapper();
        coordinator = coordinatorBuilder
                .withMaxTimeToCommitOrRollbackMillis(MAX_TIME_TO_COMMIT_OR_ROLLBACK_MILLIS)
                .withWaitPeriodBeforeSweeperUnlockMillis(WAIT_PERIOD_BEFORE_SWEEPER_UNLOCK_MILLIS)
                .withWaitPeriodBeforeSweeperDeleteMillis(WAIT_PERIOD_BEFORE_SWEEPER_DELETE_MILLIS)
                .build();
        txLogItemMapper = new TransactionLogItemMapper();
        createTable(bankAccountItemMapper);
    }

    @After
    public void destroy() {
        deleteTable(bankAccountItemMapper);
    }

    protected Map<String, AttributeValue> getRawTxLogItem(String transactionId) {
        GetItemRequest getRequest = txLogItemMapper.generateGetItemRequest(transactionId);
        return ddbClient.getItem(getRequest).getItem();

    }

    protected TransactionLogItem getTxLogItem(final String transactionId) {
        return txLogItemMapper.unmarshall(getRawTxLogItem(transactionId));
    }

    protected void overrideTransactionStatus(String transactionId, TransactionStatus transactionStatus) {
        Map<String, AttributeValue> rawTxLogItem = getRawTxLogItem(transactionId);
        rawTxLogItem.put(TransactionLogItemMapper.TRANSACTION_STATUS_KEY_NAME, new AttributeValue(transactionStatus.name()));
        PutItemRequest putRequest = new PutItemRequest(txLogItemMapper.getTableName(), rawTxLogItem);
        ddbClient.putItem(putRequest);
    }

    protected void overrideSweeperUnlockStatus(String transactionId, boolean unlockedBySweeper) {
        Map<String, AttributeValue> rawTxLogItem = getRawTxLogItem(transactionId);
        rawTxLogItem.put(TransactionLogItemMapper.UNLOCKED_BY_SWEEPER, new AttributeValue().withBOOL(unlockedBySweeper));
        PutItemRequest putRequest = new PutItemRequest(txLogItemMapper.getTableName(), rawTxLogItem);
        ddbClient.putItem(putRequest);
    }

    protected void deleteTransactionLogItem(String transactionId) {
        ddbClient.deleteItem(txLogItemMapper.generateDeleteItemRequest(transactionId));
    }

    protected Map<String, AttributeValue> getRawBankAccountItem(BankAccountItem bankAccountItem) {
        GetItemRequest getItemRequest =  bankAccountItemMapper.generateGetItemRequest(bankAccountItem.getBeneficiaryName(), bankAccountItem.getAccountType());
        getItemRequest.getAttributesToGet().add(TransactionCoordinator.TRANSACTION_ID_CONTROL_FIELD);
        getItemRequest.getAttributesToGet().add(TransactionCoordinator.TRANSACTION_OPERATION_CONTROL_FIELD);
        GetItemResult getItemResult = ddbClient.getItem(getItemRequest);
        return getItemResult.getItem();

    }

    protected void createItemWithUnlockingCommit(BankAccountItem bankAccountItem) {
        coordinator.startTransaction();
        coordinator.createItem(bankAccountItemMapper.generateUpdateItemRequest(bankAccountItem));
        List<Future> unlockFutures = coordinator.commit();
        Futures.blockOnAllFutures(unlockFutures, Instant.now().plusSeconds(1));
    }

    protected void createItemWithUnlockingCommit(String transactionId, BankAccountItem bankAccountItem) {
        coordinator.startTransaction(transactionId);
        coordinator.createItem(bankAccountItemMapper.generateUpdateItemRequest(bankAccountItem));
        List<Future> unlockFutures = coordinator.commit();
        Futures.blockOnAllFutures(unlockFutures, Instant.now().plusSeconds(1));
    }

    protected void createItemWithoutUnlockingCommit(BankAccountItem bankAccountItem) {
        coordinator.startTransaction();
        coordinator.createItem(bankAccountItemMapper.generateUpdateItemRequest(bankAccountItem));
        coordinator.commitWithoutUnlocking();
    }

    protected void createItemWithoutUnlockingCommit(String transactionId, BankAccountItem bankAccountItem) {
        coordinator.startTransaction(transactionId);
        coordinator.createItem(bankAccountItemMapper.generateUpdateItemRequest(bankAccountItem));
        coordinator.commitWithoutUnlocking();
    }

    protected void createAndDeleteItemWithoutUnlockingCommit(String transactionId, BankAccountItem toCreate, BankAccountItem toDelete) {
        coordinator.startTransaction(transactionId);
        coordinator.createItem(bankAccountItemMapper.generateUpdateItemRequest(toCreate));
        coordinator.deleteItem(bankAccountItemMapper.generateDeleteItemRequest(toDelete.getBeneficiaryName(), toDelete.getAccountType()));
        coordinator.commitWithoutUnlocking();
    }
}
