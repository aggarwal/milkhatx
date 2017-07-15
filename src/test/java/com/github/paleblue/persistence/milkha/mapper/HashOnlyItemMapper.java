package com.github.paleblue.persistence.milkha.mapper;


import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.github.paleblue.persistence.milkha.dto.HashOnlyItem;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public class HashOnlyItemMapper extends HashOnlyMapper<HashOnlyItem> {

    private static final String TABLE = "HashOnlyItems";
    private static final String HASH_KEY_FIELD = "hashKey";

    @Override
    public Map<String, AttributeValue> marshall(HashOnlyItem item) {
        return Collections.singletonMap(HASH_KEY_FIELD, new AttributeValue(item.getHashKey()));
    }

    @Override
    public HashOnlyItem unmarshall(Map<String, AttributeValue> itemAttributeMap) {
        return new HashOnlyItem(itemAttributeMap.get(TABLE).getS());
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
        return Collections.singletonMap(HASH_KEY_FIELD, new AttributeValue(hashKeyValue));
    }

    @Override
    public List<AttributeDefinition> getAttributeDefinitions() {
        return Collections.singletonList(new AttributeDefinition(HASH_KEY_FIELD, ScalarAttributeType.S));
    }
}
