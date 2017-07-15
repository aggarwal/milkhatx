package com.github.paleblue.persistence.milkha;


import java.util.UUID;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.github.paleblue.persistence.milkha.mapper.HashOnlyMapper;
import com.github.paleblue.persistence.milkha.mapper.TransactionLogItemMapper;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

@Ignore
public abstract class DynamoDBLocalAbstractTest {

    protected final AmazonDynamoDB ddbClient;

    protected DynamoDBLocalAbstractTest() {
        ddbClient = new AmazonDynamoDBClient(new BasicAWSCredentials("test", ""));
        ddbClient.setEndpoint(System.getProperty("dynamodb-local.endpoint"));
    }

    @Before
    public void createTables() {
        createTable(new TransactionLogItemMapper());
    }

    @After
    public void deleteTables() {
        deleteTable(new TransactionLogItemMapper());
    }

    protected void createTable(HashOnlyMapper mapper) {
        CreateTableRequest createTableRequest = mapper.generateCreateTableRequest();
        createTableRequest.setProvisionedThroughput(new ProvisionedThroughput(10L, 10L));
        ddbClient.createTable(createTableRequest);
    }

    protected void deleteTable(HashOnlyMapper mapper) {
        ddbClient.deleteTable(mapper.generateDeleteTableRequest());
    }

    protected String randomTransactionId() {
        return UUID.randomUUID().toString();
    }
}
