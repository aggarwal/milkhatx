package com.github.paleblue.persistence.milkha;


import java.util.UUID;
import java.util.concurrent.Executors;

import com.github.paleblue.persistence.milkha.dto.HashAndRangeOnlyItem;
import com.github.paleblue.persistence.milkha.dto.HashOnlyItem;
import com.github.paleblue.persistence.milkha.mapper.HashAndRangeOnlyItemMapper;
import com.github.paleblue.persistence.milkha.mapper.HashOnlyItemMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TransactionCoordinatorEmptyItemPersistenceTest extends DynamoDBLocalAbstractTest {

    private TransactionCoordinator coordinator;
    private TransactionCoordinatorBuilder coordinatorBuilder;
    private HashOnlyItem hashOnlyItem;
    private HashOnlyItemMapper hashOnlyItemMapper;
    private HashAndRangeOnlyItem hashAndRangeOnlyItem;
    private HashAndRangeOnlyItemMapper hashAndRangeOnlyItemMapper;

    public TransactionCoordinatorEmptyItemPersistenceTest() {
        this.coordinatorBuilder = new TransactionCoordinatorBuilder(ddbClient, Executors.newSingleThreadExecutor());
        this.hashAndRangeOnlyItemMapper = new HashAndRangeOnlyItemMapper();
        this.hashOnlyItemMapper = new HashOnlyItemMapper();
    }

    @Before
    public void setup() {
        coordinator = coordinatorBuilder.build();
        hashOnlyItem = new HashOnlyItem(UUID.randomUUID().toString());
        hashAndRangeOnlyItem = new HashAndRangeOnlyItem(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        createTable(hashOnlyItemMapper);
        createTable(hashAndRangeOnlyItemMapper);
    }

    @After
    public void destroy() {
        deleteTable(hashOnlyItemMapper);
        deleteTable(hashAndRangeOnlyItemMapper);
    }

    @Test
    public void hashOnlyItemCanBePersistedRetrievedAndDeleted() {
        coordinator.startTransaction();
        coordinator.createItem(hashOnlyItemMapper.generateUpdateItemRequest(hashOnlyItem));
        coordinator.commit();
        final String hashKey = hashOnlyItem.getHashKey();
        coordinator.query(hashOnlyItemMapper.generateQueryRequest(hashKey));
        coordinator.startTransaction();
        coordinator.scan(hashOnlyItemMapper.generateScanRequest());
        coordinator.deleteItem(hashOnlyItemMapper.generateDeleteItemRequest(hashKey));
        coordinator.commit();
    }

    @Test
    public void hashAndRangeOnlyItemCanBePersistedRetrievedAndDeleted() {
        coordinator.startTransaction();
        coordinator.createItem(hashAndRangeOnlyItemMapper.generateUpdateItemRequest(hashAndRangeOnlyItem));
        coordinator.commit();
        final String hashKey = hashAndRangeOnlyItem.getHashKey();
        final String rangekey = hashAndRangeOnlyItem.getRangeKey();
        coordinator.query(hashAndRangeOnlyItemMapper.generateQueryRequest(hashKey, rangekey));
        coordinator.scan(hashAndRangeOnlyItemMapper.generateScanRequest());
        coordinator.startTransaction();
        coordinator.deleteItem(hashAndRangeOnlyItemMapper.generateDeleteItemRequest(hashKey, rangekey));
        coordinator.commit();
    }
}
