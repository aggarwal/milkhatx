package com.github.paleblue.persistence.milkha;


import java.io.File;
import java.util.UUID;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
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
        System.out.println("PROPERTY" + System.getProperty("sqlite4java.library.path"));
        // System.setProperty("sqlite4java.library.path", System.getProperty("user.dir")+"/lib/sqlite4java-392/");
        ddbClient = DynamoDBEmbedded.create(new File("/Users/shitanshu/ddblocal.sqlite")).amazonDynamoDB();
    }

    @Before
    public void createTables() {
        createTable(new TransactionLogItemMapper());
    }

    @After
    public void deleteTables() {
        deleteTable(new TransactionLogItemMapper());
        ddbClient.shutdown();
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
