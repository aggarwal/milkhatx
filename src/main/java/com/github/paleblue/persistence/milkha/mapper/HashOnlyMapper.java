package com.github.paleblue.persistence.milkha.mapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.util.StringUtils;


public abstract class HashOnlyMapper<T> {

    public abstract Map<String, AttributeValue> marshall(final T item);

    public abstract T unmarshall(final Map<String, AttributeValue> itemAttributeMap);

    public abstract String getTableName();

    public abstract String getHashKeyName();

    public abstract Map<String, AttributeValue> getPrimaryKeyMap(String hashKeyValue);

    public abstract List<AttributeDefinition> getAttributeDefinitions();

    public List<String> getAttributeNames() {
        List<AttributeDefinition> attributeDefinitions = getAttributeDefinitions();
        List<String> attributeNames = new ArrayList<>(attributeDefinitions.size());
        for (AttributeDefinition attributeDefinition : attributeDefinitions) {
            attributeNames.add(attributeDefinition.getAttributeName());
        }
        return attributeNames;
    }

    public CreateTableRequest generateCreateTableRequest() {
        return new CreateTableRequest().
                withTableName(getTableName()).
                withKeySchema(getKeySchema()).
                withAttributeDefinitions(Arrays.asList(new AttributeDefinition(getHashKeyName(), ScalarAttributeType.S)));
    }

    public List<KeySchemaElement> getKeySchema() {
        return Arrays.asList(new KeySchemaElement(getHashKeyName(), KeyType.HASH));
    }

    public DeleteTableRequest generateDeleteTableRequest() {
        return new DeleteTableRequest().withTableName(getTableName());
    }

    public GetItemRequest generateGetItemRequest(String hashKeyValue) {
        return new GetItemRequest().
                withConsistentRead(true).
                withTableName(getTableName()).
                withKey(getPrimaryKeyMap(hashKeyValue)).
                withAttributesToGet(getAttributeNames());
    }

    public PutItemRequest generatePutItemRequest(T item) {
        return new PutItemRequest(getTableName(), marshall(item));
    }

    public UpdateItemRequest generateUpdateItemRequest(T item) {
        Map<String, AttributeValue> rawItem = marshall(item);
        Map<String, String> expressionAttributeNames = new HashMap<>(rawItem.size());
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>(rawItem.size());
        List<String> setStatements = new ArrayList<>(rawItem.size());
        for (Map.Entry<String, AttributeValue> entry : rawItem.entrySet()) {
            String attributeName = entry.getKey();
            if (getHashKeyName().equals(attributeName)) {
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
        Map<String, AttributeValue> key = getPrimaryKeyMap(rawItem.get(getHashKeyName()).getS());
        return new UpdateItemRequest().
                withTableName(getTableName()).
                withExpressionAttributeNames(expressionAttributeNames).
                withExpressionAttributeValues(expressionAttributeValues).
                withUpdateExpression(updateExpression).
                withKey(key);
    }

    public DeleteItemRequest generateDeleteItemRequest(String hashKeyValue) {
        return new DeleteItemRequest().
                withTableName(getTableName()).
                withKey(getPrimaryKeyMap(hashKeyValue));
    }

    public QueryRequest generateQueryRequest(String hashKeyValue) {
        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#hashkey", getHashKeyName());
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":hashvalue", new AttributeValue(hashKeyValue));
        return new QueryRequest().
                withConsistentRead(true).
                withTableName(getTableName()).
                withExpressionAttributeNames(expressionAttributeNames).
                withExpressionAttributeValues(expressionAttributeValues).
                withKeyConditionExpression("#hashkey = :hashvalue");
    }

    public ScanRequest generateScanRequest() {
        return new ScanRequest().withTableName(getTableName());
    }
}
