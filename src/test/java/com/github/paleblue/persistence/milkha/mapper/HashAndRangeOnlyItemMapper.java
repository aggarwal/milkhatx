package com.github.paleblue.persistence.milkha.mapper;


import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.paleblue.persistence.milkha.dto.HashAndRangeOnlyItem;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public class HashAndRangeOnlyItemMapper extends HashAndRangeMapper<HashAndRangeOnlyItem> {

    private static final String TABLE = "HashAndRangeOnlyItems";
    private static final String HASH_KEY_FIELD = "hashKey";
    private static final String RANGE_KEY_FIELD = "rangeKey";

    @Override
    public String getRangeKeyName() {
        return RANGE_KEY_FIELD;
    }

    @Override
    public Map<String, AttributeValue> getPrimaryKeyMap(String hashKeyValue, String rangeKeyValue) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(HASH_KEY_FIELD, new AttributeValue(hashKeyValue));
        key.put(RANGE_KEY_FIELD, new AttributeValue(rangeKeyValue));
        return key;
    }

    @Override
    public List<LocalSecondaryIndex> getLocalSecondaryIndices() {
        return Collections.emptyList();
    }

    @Override
    public Map<String, AttributeValue> marshall(HashAndRangeOnlyItem item) {
        Map<String, AttributeValue> itemMap = new HashMap<>();
        itemMap.put(HASH_KEY_FIELD, new AttributeValue(item.getHashKey()));
        itemMap.put(RANGE_KEY_FIELD, new AttributeValue(item.getRangeKey()));
        return itemMap;
    }

    @Override
    public HashAndRangeOnlyItem unmarshall(Map<String, AttributeValue> itemAttributeMap) {
        return new HashAndRangeOnlyItem(
                itemAttributeMap.get(HASH_KEY_FIELD).getS(),
                itemAttributeMap.get(RANGE_KEY_FIELD).getS());
    }

    @Override
    public String getTableName() {
        return TABLE;
    }

    @Override
    public String getHashKeyName() {
        return HASH_KEY_FIELD;
    }

    @Override
    public Map<String, AttributeValue> getPrimaryKeyMap(String hashKeyValue) {
        throw new UnsupportedOperationException("This table needs a range key.");
    }

    @Override
    public List<AttributeDefinition> getAttributeDefinitions() {
        return Arrays.asList(
                new AttributeDefinition(HASH_KEY_FIELD, ScalarAttributeType.S),
                new AttributeDefinition(RANGE_KEY_FIELD, ScalarAttributeType.S));
    }
}
