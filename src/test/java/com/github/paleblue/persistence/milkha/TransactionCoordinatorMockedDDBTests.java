package com.github.paleblue.persistence.milkha;


import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.github.paleblue.persistence.milkha.TransactionCoordinator;
import com.github.paleblue.persistence.milkha.TransactionCoordinatorBuilder;
import com.github.paleblue.persistence.milkha.dto.BankAccountItem;
import com.github.paleblue.persistence.milkha.mapper.BankAccountItemMapper;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TransactionCoordinatorMockedDDBTests {

    @Mock
    private AmazonDynamoDBClient ddbClient;

    private TransactionCoordinator coordinator;
    private TransactionCoordinatorBuilder coordinatorBuilder;
    private BankAccountItemMapper bankAccountItemMapper;
    private BankAccountItem drEvilSavingsAccount;
    private Random random;

    public TransactionCoordinatorMockedDDBTests() {
        random = new Random();
    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        String beneficiaryName = String.format("DrEvil-%s", UUID.randomUUID().toString());
        Integer usdAmount = random.nextInt(1000000);
        String accountType = "savings";
        drEvilSavingsAccount = new BankAccountItem(beneficiaryName, accountType, usdAmount);
        bankAccountItemMapper = new BankAccountItemMapper();
        coordinatorBuilder = new TransactionCoordinatorBuilder(ddbClient, Executors.newSingleThreadExecutor());
        coordinator = coordinatorBuilder.build();
    }

    @Test
    public void whenCoordinatorFailsToSetTransactionStatusToCommitThenCoordinatorCanRollback() {
        when(ddbClient.putItem(any(PutItemRequest.class)))
                .thenReturn(new PutItemResult())
                .thenThrow(new ProvisionedThroughputExceededException("Throughput exceeded!"))
                .thenReturn(new PutItemResult());
        try {
            coordinator.startTransaction();
            coordinator.createItem(bankAccountItemMapper.generateUpdateItemRequest(drEvilSavingsAccount));
            coordinator.commit();
        } catch (ProvisionedThroughputExceededException e) {
            coordinator.rollback();
            verify(ddbClient, times(1)).updateItem(any(UpdateItemRequest.class));
            verify(ddbClient, times(1)).deleteItem(any(DeleteItemRequest.class));
            verify(ddbClient, times(3)).putItem(any(PutItemRequest.class));
            return;
        }
        fail("Exception block should have been reached");
    }
}
