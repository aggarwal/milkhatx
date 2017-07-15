package com.github.paleblue.persistence.milkha.mapper;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.util.StringUtils;

public abstract class HashAndRangeMapper<T> extends HashOnlyMapper<T> {

    public abstract String getRangeKeyName();

    public abstract Map<String, AttributeValue> getPrimaryKeyMap(String hashKeyValue, String rangeKeyValue);

    public abstract List<LocalSecondaryIndex> getLocalSecondaryIndices();

    public GetItemRequest generateGetItemRequest(String hashKeyValue, String rangeKeyValue) {
        return new GetItemRequest().
                withConsistentRead(true).
                withTableName(getTableName()).
                withKey(getPrimaryKeyMap(hashKeyValue, rangeKeyValue)).
                withAttributesToGet(getAttributeNames());
    }

    public DeleteItemRequest generateDeleteItemRequest(String hashKeyValue, String rangeKeyValue) {
        return new DeleteItemRequest().
                withTableName(getTableName()).
                withKey(getPrimaryKeyMap(hashKeyValue, rangeKeyValue));
    }

    public QueryRequest generateQueryRequest(String hashKeyValue, String rangeKeyValue) {
        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#hashkey", getHashKeyName());
        expressionAttributeNames.put("#rangekey", getRangeKeyName());
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":hashvalue", new AttributeValue(hashKeyValue));
        expressionAttributeValues.put(":rangevalue", new AttributeValue(rangeKeyValue));
        return new QueryRequest().
                withConsistentRead(true).
                withTableName(getTableName()).
                withExpressionAttributeNames(expressionAttributeNames).
                withExpressionAttributeValues(expressionAttributeValues).
                withKeyConditionExpression("#hashkey = :hashvalue AND #rangekey = :rangevalue");
    }

    @Override
    public CreateTableRequest generateCreateTableRequest() {
        Collection<LocalSecondaryIndex> lsi = getLocalSecondaryIndices();
        if (lsi.isEmpty()) {
            lsi = null;
        }
        return new CreateTableRequest().
                withTableName(getTableName()).
                withKeySchema(Arrays.asList(new KeySchemaElement(getHashKeyName(), KeyType.HASH),
                        new KeySchemaElement(getRangeKeyName(), KeyType.RANGE))).
                withAttributeDefinitions(getAttributeDefinitions()).
                withLocalSecondaryIndexes(lsi);
    }

    @Override
    public UpdateItemRequest generateUpdateItemRequest(T item) {
        Map<String, AttributeValue> rawItem = marshall(item);
        Map<String, String> expressionAttributeNames = new HashMap<>(rawItem.size());
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>(rawItem.size());
        List<String> setStatements = new ArrayList<>(rawItem.size());
        for (Map.Entry<String, AttributeValue> entry : rawItem.entrySet()) {
            String attributeName = entry.getKey();
            if (getHashKeyName().equals(attributeName) || getRangeKeyName().equals(attributeName)) {
                continue;
            }
            AttributeValue attributeValue = entry.getValue();
            String placeholderNameForAttributeName = "#" + attributeName;
            String placeholderNameForAttributeValue = ":" + attributeName;
            expressionAttributeNames.put(placeholderNameForAttributeName, attributeName);
            expressionAttributeValues.put(placeholderNameForAttributeValue, attributeValue);
            setStatements.add(String.format("%s = %s", placeholderNameForAttributeName, placeholderNameForAttributeValue));
        }
        String updateExpression = null;
        if (!setStatements.isEmpty()) {
            updateExpression = "SET " + StringUtils.join(", ", setStatements.toArray(new String[setStatements.size()]));
        }
        Map<String, AttributeValue> key = getPrimaryKeyMap(rawItem.get(getHashKeyName()).getS(), rawItem.get(getRangeKeyName()).getS());
        return new UpdateItemRequest().
                withTableName(getTableName()).
                withExpressionAttributeNames(expressionAttributeNames).
                withExpressionAttributeValues(expressionAttributeValues).
                withUpdateExpression(updateExpression).
                withKey(key);
    }
}
