package com.github.paleblue.persistence.milkha;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.github.paleblue.persistence.milkha.dto.BankAccountItem;
import com.github.paleblue.persistence.milkha.dto.TransactionLogItem;
import com.github.paleblue.persistence.milkha.dto.TransactionStatus;
import com.github.paleblue.persistence.milkha.exception.TransactionTimedOutException;
import com.github.paleblue.persistence.milkha.mapper.BankAccountItemMapper;
import com.github.paleblue.persistence.milkha.util.Futures;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;

@RunWith(MockitoJUnitRunner.class)
public class TransactionCoordinatorTimeoutTest extends TransactionCoordinatorBaseTest {
    private BankAccountItem drEvilSavingsAccount;

    @Mock
    AmazonDynamoDBClient mockDDBClient;

    @Before
    public void setup() {
        Mockito.when(mockDDBClient.putItem(any(PutItemRequest.class))).thenAnswer(invocation -> {
            Thread.sleep(100L);
            return new PutItemResult();
        });
        String beneficiaryName = String.format("DrEvil-%s", UUID.randomUUID().toString());
        Integer usdAmount = random.nextInt(1000000);
        drEvilSavingsAccount = new BankAccountItem(beneficiaryName, SAVINGS_ACCOUNT_TYPE, usdAmount);
    }

    @Test(expected = TransactionTimedOutException.class)
    public void whenCommitTimesOutThenThrowException() {
        TransactionCoordinator coordinator = coordinatorBuilder
                .withAmazonDynamoDBClient(mockDDBClient)
                .withMaxTimeToCommitOrRollbackMillis(100L)
                .build();
        coordinator.startTransaction();
        coordinator.createItem(bankAccountItemMapper.generateUpdateItemRequest(drEvilSavingsAccount));
        coordinator.commit();
    }

    @Test
    public void whenCommitDoesNotTimeOutThenCommitIsSuccessful() {
        TransactionCoordinator coordinator = coordinatorBuilder
                .withMaxTimeToCommitOrRollbackMillis(2000L)
                .build();
        coordinator.startTransaction();
        coordinator.createItem(bankAccountItemMapper.generateUpdateItemRequest(drEvilSavingsAccount));
        coordinator.commit();
        QueryRequest queryRequest = bankAccountItemMapper.generateQueryRequest(drEvilSavingsAccount.getBeneficiaryName(), drEvilSavingsAccount.getAccountType());
        QueryResult queryResult = coordinator.query(queryRequest);
        List<Map<String, AttributeValue>> committedItems = queryResult.getItems();
        assertEquals(1, committedItems.size());
        Map<String, AttributeValue> committedItem = committedItems.get(0);
        assertEquals(drEvilSavingsAccount.getBeneficiaryName(), committedItem.get(BankAccountItemMapper.ACCOUNT_BENEFICIARY_KEY_NAME).getS());
        assertEquals(drEvilSavingsAccount.getTotalAmountInUsd(), new Integer(committedItem.get(BankAccountItemMapper.TOTAL_AMOUNT_IN_USD_KEY_NAME).getN()));
    }

    @Test(expected = TransactionTimedOutException.class)
    public void whenRollbackTimesOutThenThrowException() {
        TransactionCoordinator coordinator = coordinatorBuilder
                .withAmazonDynamoDBClient(mockDDBClient)
                .withMaxTimeToCommitOrRollbackMillis(50L)
                .build();
        String transactionId = randomTransactionId();
        coordinator.startTransaction(transactionId);
        TransactionLogItem txLogItem = new TransactionLogItem(transactionId, TransactionStatus.START_COMMIT, WAIT_PERIOD_BEFORE_SWEEPER_UNLOCK_MILLIS,
                WAIT_PERIOD_BEFORE_SWEEPER_DELETE_MILLIS);
        Mockito.when(mockDDBClient.getItem(any(GetItemRequest.class))).thenAnswer(invocation -> {
            return new GetItemResult().withItem(txLogItemMapper.marshall(txLogItem));
        });
        coordinator.reloadTransactionLogItem();
        coordinator.rollback();
    }

    @Test
    public void whenRollbackDoesNotTimeThenRollbackIsSuccessful() {
        TransactionCoordinator coordinator = coordinatorBuilder
                .withMaxTimeToCommitOrRollbackMillis(2000L)
                .build();
        String transactionId = randomTransactionId();
        coordinator.startTransaction(transactionId);
        coordinator.createItem(bankAccountItemMapper.generateUpdateItemRequest(drEvilSavingsAccount));
        coordinator.commitWithoutUnlocking();
        QueryRequest queryRequest = bankAccountItemMapper.generateQueryRequest(drEvilSavingsAccount.getBeneficiaryName(), drEvilSavingsAccount.getAccountType());
        QueryResult queryResult = coordinator.query(queryRequest);
        assertEquals(1, queryResult.getItems().size());
        overrideTransactionStatus(transactionId, TransactionStatus.START_COMMIT);
        coordinator.reloadTransactionLogItem();
        List<Future> rollbackUnlockFutures = coordinator.rollback();
        Futures.blockOnAllFutures(rollbackUnlockFutures, Instant.now().plusSeconds(1));
        queryResult = coordinator.query(queryRequest);
        assertEquals(0, queryResult.getItems().size());
    }
}
