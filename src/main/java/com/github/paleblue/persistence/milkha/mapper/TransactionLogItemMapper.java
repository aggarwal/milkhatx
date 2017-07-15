package com.github.paleblue.persistence.milkha.mapper;

import static com.github.paleblue.persistence.milkha.util.Preconditions.checkNotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.github.paleblue.persistence.milkha.dto.TransactionLogItem;
import com.github.paleblue.persistence.milkha.dto.TransactionStatus;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;


public class TransactionLogItemMapper extends HashOnlyMapper<TransactionLogItem> {

    public static final String TRANSACTION_ID_KEY_NAME = "transactionId";
    public static final String TRANSACTION_STATUS_KEY_NAME = "transactionStatus";
    public static final String WAIT_PERIOD_BEFORE_SWEEPER_UNLOCK_MILLIS = "waitPeriodBeforeSweeperUnlockMillis";
    public static final String WAIT_PERIOD_BEFORE_SWEEPER_DELETE_MILLIS = "waitPeriodBeforeSweeperDeleteMillis";
    public static final String UNLOCKED_BY_SWEEPER = "unlockedBySweeper";
    public static final String TRANSACTION_LOG_TABLE_NAME = "TransactionLog";

    private static final String CREATE_SET_KEY_NAME = "createSet";
    private static final String DELETE_SET_KEY_NAME = "deleteSet";

    @Override
    public Map<String, AttributeValue> marshall(TransactionLogItem item) {
        checkNotNull(item);
        Map<String, AttributeValue> attributeMap = new HashMap<>();
        attributeMap.put(TRANSACTION_ID_KEY_NAME, new AttributeValue(item.getTransactionId()));
        attributeMap.put(TRANSACTION_STATUS_KEY_NAME, new AttributeValue(item.getTransactionStatus().name()));
        attributeMap.put(WAIT_PERIOD_BEFORE_SWEEPER_UNLOCK_MILLIS, new AttributeValue().withN(String.valueOf(item.getWaitPeriodBeforeSweeperUnlockMillis())));
        attributeMap.put(WAIT_PERIOD_BEFORE_SWEEPER_DELETE_MILLIS, new AttributeValue().withN(String.valueOf(item.getWaitPeriodBeforeSweeperDeleteMillis())));
        attributeMap.put(UNLOCKED_BY_SWEEPER, new AttributeValue().withBOOL(item.isUnlockedBySweeper()));
        if (item.getCreateSet() != null && !item.getCreateSet().isEmpty()) {
            attributeMap.put(CREATE_SET_KEY_NAME, toAttributeValue(item.getCreateSet()));
        }
        if (item.getDeleteSet() != null && !item.getDeleteSet().isEmpty()) {
            attributeMap.put(DELETE_SET_KEY_NAME, toAttributeValue(item.getDeleteSet()));
        }
        return attributeMap;
    }

    @Override
    public TransactionLogItem unmarshall(Map<String, AttributeValue> attributeMap) {
        checkNotNull(attributeMap);
        String transactionId = attributeMap.get(TRANSACTION_ID_KEY_NAME).getS();
        TransactionStatus transactionStatus = TransactionStatus.valueOf(attributeMap.get(TRANSACTION_STATUS_KEY_NAME).getS());
        Long waitPeriodBeforeSweeperUnlockMillis = Long.parseLong(attributeMap.get(WAIT_PERIOD_BEFORE_SWEEPER_UNLOCK_MILLIS).getN());
        Long waitPeriodBeforeSweeperDeleteMillis = Long.parseLong(attributeMap.get(WAIT_PERIOD_BEFORE_SWEEPER_DELETE_MILLIS).getN());
        boolean unlockedBySweeper = false;
        if (attributeMap.containsKey(UNLOCKED_BY_SWEEPER)) {
            unlockedBySweeper = attributeMap.get(UNLOCKED_BY_SWEEPER).getBOOL();
        }
        Map<String, List<Map<String, AttributeValue>>> createSet = null;
        if (attributeMap.containsKey(CREATE_SET_KEY_NAME)) {
            createSet = commitSetfromAttributeValue(attributeMap.get(CREATE_SET_KEY_NAME).getM());
        }
        Map<String, List<Map<String, AttributeValue>>> deleteSet = null;
        if (attributeMap.containsKey(DELETE_SET_KEY_NAME)) {
            deleteSet = commitSetfromAttributeValue(attributeMap.get(DELETE_SET_KEY_NAME).getM());
        }
        return new TransactionLogItem(transactionId, transactionStatus, waitPeriodBeforeSweeperUnlockMillis, waitPeriodBeforeSweeperDeleteMillis, unlockedBySweeper, createSet, deleteSet);
    }

    @Override
    public String getTableName() {
        return TRANSACTION_LOG_TABLE_NAME;
    }

    @Override
    public String getHashKeyName() {
        return TRANSACTION_ID_KEY_NAME;
    }

    @Override
    public Map<String, AttributeValue> getPrimaryKeyMap(String transactionId) {
        return Collections.singletonMap(TRANSACTION_ID_KEY_NAME, new AttributeValue(transactionId));
    }

    @Override
    public List<AttributeDefinition> getAttributeDefinitions() {
        return  Arrays.asList(
                new AttributeDefinition(TRANSACTION_ID_KEY_NAME, ScalarAttributeType.S),
                new AttributeDefinition(TRANSACTION_STATUS_KEY_NAME, ScalarAttributeType.S),
                new AttributeDefinition(WAIT_PERIOD_BEFORE_SWEEPER_UNLOCK_MILLIS, ScalarAttributeType.N),
                new AttributeDefinition(WAIT_PERIOD_BEFORE_SWEEPER_DELETE_MILLIS, ScalarAttributeType.N),
                new AttributeDefinition(UNLOCKED_BY_SWEEPER, ScalarAttributeType.N),
                new AttributeDefinition(CREATE_SET_KEY_NAME, "M"),
                new AttributeDefinition(DELETE_SET_KEY_NAME, "M"));
    }

    private AttributeValue toAttributeValue(Map<String, List<Map<String, AttributeValue>>> commitSet) {
        Map<String, AttributeValue> tableToKeys = new HashMap<>();
        for (Map.Entry<String, List<Map<String, AttributeValue>>> entry : commitSet.entrySet()) {
            List<AttributeValue> keys = entry.getValue().stream().map(key -> new AttributeValue().withM(key)).collect(Collectors.toList());
            tableToKeys.put(entry.getKey(), new AttributeValue().withL(keys));
        }
        return new AttributeValue().withM(tableToKeys);
    }

    private Map<String, List<Map<String, AttributeValue>>> commitSetfromAttributeValue(Map<String, AttributeValue> rawMap) {
        Map<String, List<Map<String, AttributeValue>>> tableToKeys = new HashMap<>();
        rawMap.forEach((tableName, keyList) ->
                tableToKeys.put(tableName, keyList.getL().stream().map(AttributeValue::getM).collect(Collectors.toList())));
        return tableToKeys;
    }
}
