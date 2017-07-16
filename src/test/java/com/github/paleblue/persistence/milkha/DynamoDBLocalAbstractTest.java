package com.github.paleblue.persistence.milkha;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.github.paleblue.persistence.milkha.mapper.HashOnlyMapper;
import com.github.paleblue.persistence.milkha.mapper.TransactionLogItemMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

@Ignore
public abstract class DynamoDBLocalAbstractTest {

    protected final AmazonDynamoDB ddbClient;

    protected DynamoDBLocalAbstractTest() {
        ddbClient = DynamoDBEmbedded.create().amazonDynamoDB();
    }

    @BeforeClass
    public static void setSqlitePath() {
        final String sqliteJarPath = System.getProperty("sqlite4java.jar.path");
        if (sqliteJarPath == null) {
            return;
        }
        final Path sqliteJar = Paths.get(sqliteJarPath);
        final String sqliteNativePath;
        switch (System.getProperty("os.name")) {
            case "Mac OS X":
                int length = sqliteJar.getNameCount();
                final String version = sqliteJar.getName(length-2).toString();
                final String groupPath = sqliteJar.getParent().getParent().getParent().toString();
                sqliteNativePath = Paths.get(groupPath, "libsqlite4java-osx", version).toString();
                break;
            default:
                sqliteNativePath = sqliteJar.toString();
        }
        System.setProperty("sqlite4java.library.path", sqliteNativePath);
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
