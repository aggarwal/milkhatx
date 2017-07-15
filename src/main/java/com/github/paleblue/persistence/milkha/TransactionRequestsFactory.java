package com.github.paleblue.persistence.milkha;


import static com.github.paleblue.persistence.milkha.util.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonWebServiceRequest;
import com.github.paleblue.persistence.milkha.dto.TransactionLogItem;
import com.github.paleblue.persistence.milkha.dto.TransactionStatus;
import com.github.paleblue.persistence.milkha.mapper.TransactionLogItemMapper;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.util.ImmutableMapParameter;
import com.amazonaws.util.StringUtils;

final class TransactionRequestsFactory {

    private TransactionLogItem txLogItem;
    private TransactionLogItemMapper txLogItemMapper;

    protected TransactionRequestsFactory(TransactionLogItem txLogItem) {
        this.txLogItem = txLogItem;
        this.txLogItemMapper = new TransactionLogItemMapper();
    }

    protected List<AmazonWebServiceRequest> generatePostRollbackUnlockRequestsFromCommitSets() {
        List<AmazonWebServiceRequest> unlockRequests = new ArrayList<>();
        txLogItem.getCreateSet().forEach((tableName, keys) -> keys.stream().forEach(key -> unlockRequests.add(generatePostRollbackUnlockRequestForAdd(tableName, key))));
        txLogItem.getDeleteSet().forEach((tableName, keys) -> keys.stream().forEach(key -> unlockRequests.add(generatePostRollbackUnlockRequestForDelete(tableName, key))));
        return unlockRequests;
    }

    protected List<AmazonWebServiceRequest> generatePostCommitUnlockRequestsFromCommitSets() {
        List<AmazonWebServiceRequest> unlockRequests = new ArrayList<>();
        txLogItem.getCreateSet().forEach((tableName, keys) -> keys.stream().forEach(key -> unlockRequests.add(generatePostCommitUnlockRequestForAdd(tableName, key))));
        txLogItem.getDeleteSet().forEach((tableName, keys) -> keys.stream().forEach(key -> unlockRequests.add(generatePostCommitUnlockRequestForDelete(tableName, key))));
        return unlockRequests;
    }

    protected DeleteItemRequest generatePostCommitUnlockRequestForDelete(UpdateItemRequest updateItemRequest) {
        return generatePostCommitUnlockRequestForDelete(updateItemRequest.getTableName(), updateItemRequest.getKey());
    }

    private DeleteItemRequest generatePostCommitUnlockRequestForDelete(String tableName, Map<String, AttributeValue> key) {
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":txId", new AttributeValue(txLogItem.getTransactionId()));
        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#txId", TransactionCoordinator.TRANSACTION_ID_CONTROL_FIELD);
        return new DeleteItemRequest().
                withTableName(tableName).
                withExpressionAttributeNames(expressionAttributeNames).
                withExpressionAttributeValues(expressionAttributeValues).
                withConditionExpression("#txId = :txId").
                withKey(key);
    }

    private DeleteItemRequest generatePostRollbackUnlockRequestForAdd(String tableName, Map<String, AttributeValue> key) {
        return generatePostCommitUnlockRequestForDelete(tableName, key); // The underlying request is the same
    }

    protected DeleteItemRequest generatePostRollbackUnlockRequestForAdd(UpdateItemRequest updateItemRequest) {
        return generatePostCommitUnlockRequestForDelete(updateItemRequest); // The underlying request is the same
    }

    private UpdateItemRequest generatePostCommitUnlockRequestForAdd(String tableName, Map<String, AttributeValue> key) {
        return generateRequestToClearTransactionIdAndOperation(tableName, key);
    }

    protected UpdateItemRequest generatePostCommitUnlockRequestForAdd(UpdateItemRequest updateItemRequest) {
        return generateRequestToClearTransactionIdAndOperation(updateItemRequest);
    }

    private UpdateItemRequest generatePostRollbackUnlockRequestForDelete(String tableName, Map<String, AttributeValue> key) {
        return generateRequestToClearTransactionIdAndOperation(tableName, key);
    }

    protected UpdateItemRequest generatePostRollbackUnlockRequestForDelete(UpdateItemRequest updateItemRequest) {
        return generateRequestToClearTransactionIdAndOperation(updateItemRequest);
    }

    private UpdateItemRequest generateRequestToClearTransactionIdAndOperation(UpdateItemRequest updateItemRequest) {
        return generateRequestToClearTransactionIdAndOperation(updateItemRequest.getTableName(), updateItemRequest.getKey());
    }

    private UpdateItemRequest generateRequestToClearTransactionIdAndOperation(String tableName, Map<String, AttributeValue> key) {
        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#txId", TransactionCoordinator.TRANSACTION_ID_CONTROL_FIELD);
        expressionAttributeNames.put("#txOp", TransactionCoordinator.TRANSACTION_OPERATION_CONTROL_FIELD);
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":txId", new AttributeValue(txLogItem.getTransactionId()));
        String conditionExpression = "#txId = :txId";
        String updateExpression = "REMOVE #txId, #txOp";
        return new UpdateItemRequest().
                withTableName(tableName).
                withExpressionAttributeNames(expressionAttributeNames).
                withExpressionAttributeValues(expressionAttributeValues).
                withKey(key).
                withConditionExpression(conditionExpression).
                withUpdateExpression(updateExpression);
    }

    protected UpdateItemRequest generateCommitRequestForAdd(UpdateItemRequest updateItemRequest) {
        if (updateItemRequest.getConditionExpression() != null) {
            throw new IllegalArgumentException("ConditionExpression is not applicable to immutable item puts");
        }
        Map<String, String> expressionAttributeNames = new HashMap<>(updateItemRequest.getExpressionAttributeNames());
        expressionAttributeNames.put("#txId", TransactionCoordinator.TRANSACTION_ID_CONTROL_FIELD);
        expressionAttributeNames.put("#txOp", TransactionCoordinator.TRANSACTION_OPERATION_CONTROL_FIELD);
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>(updateItemRequest.getExpressionAttributeValues());
        expressionAttributeValues.put(":txId", new AttributeValue(txLogItem.getTransactionId()));
        expressionAttributeValues.put(":txOp", new AttributeValue(TransactionCoordinator.TRANSACTION_OPERATION_ADD_VALUE));
        String controlFieldUpdates = "#txId = :txId, #txOp = :txOp";
        String updateExpression;
        if (StringUtils.isNullOrEmpty(updateItemRequest.getUpdateExpression())) {
            updateExpression = "SET " + controlFieldUpdates;
        } else {
            updateExpression = updateItemRequest.getUpdateExpression() + ", " + controlFieldUpdates;
        }
        String conditionExpression = getKeyNotExistsConditionExpression(updateItemRequest.getKey());
        return new UpdateItemRequest().
                withTableName(updateItemRequest.getTableName()).
                withExpressionAttributeValues(expressionAttributeValues).
                withExpressionAttributeNames(expressionAttributeNames).
                withConditionExpression(conditionExpression).
                withKey(new HashMap<>(updateItemRequest.getKey())).
                withUpdateExpression(updateExpression);
    }

    private String getKeyNotExistsConditionExpression(Map<String, AttributeValue> keyMap) {
        String[] keyAttributeNames = keyMap.keySet().toArray(new String[]{});
        String conditionExpression = String.format("attribute_not_exists (%s)", keyAttributeNames[0]);
        if (keyAttributeNames.length == 2) {
            conditionExpression += String.format(" AND attribute_not_exists (%s)", keyAttributeNames[1]);
        }
        return conditionExpression;
    }

    private String getKeyExistsConditionExpression(Map<String, AttributeValue> keyMap) {
        String[] keyAttributeNames = keyMap.keySet().toArray(new String[]{});
        String conditionExpression = String.format("attribute_exists (%s)", keyAttributeNames[0]);
        if (keyAttributeNames.length == 2) {
            conditionExpression += String.format(" AND attribute_exists (%s)", keyAttributeNames[1]);
        }
        return conditionExpression;
    }

    protected UpdateItemRequest generateCommitRequestForDelete(DeleteItemRequest deleteItemRequest) {
        if (deleteItemRequest.getConditionExpression() != null) {
            throw new IllegalArgumentException("Setting ConditionExpression is not allowed because they cannot be distinguished from contention");
        }
        Map<String, AttributeValue> itemKey = deleteItemRequest.getKey();
        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#txId", TransactionCoordinator.TRANSACTION_ID_CONTROL_FIELD);
        expressionAttributeNames.put("#txOp", TransactionCoordinator.TRANSACTION_OPERATION_CONTROL_FIELD);
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":txId", new AttributeValue(txLogItem.getTransactionId()));
        expressionAttributeValues.put(":txOp", new AttributeValue(TransactionCoordinator.TRANSACTION_OPERATION_DELETE_VALUE));
        String keyExistsConditionExpression = getKeyExistsConditionExpression(deleteItemRequest.getKey());
        String conditionExpression = "attribute_not_exists (#txId) AND attribute_not_exists (#txOp) AND " + keyExistsConditionExpression;
        String updateExpression = "SET #txId = :txId, #txOp = :txOp";
        return new UpdateItemRequest().
                withTableName(deleteItemRequest.getTableName()).
                withExpressionAttributeNames(expressionAttributeNames).
                withExpressionAttributeValues(expressionAttributeValues).
                withKey(itemKey).
                withConditionExpression(conditionExpression).
                withUpdateExpression(updateExpression);
    }

    protected PutItemRequest generatePutRequestForTransactionLogItem() {
        checkNotNull(txLogItem.getPreviousTransactionStatus());
        if (txLogItem.getPreviousTransactionStatus() == TransactionStatus.NOT_PERSISTED) {
            return txLogItemMapper.generatePutItemRequest(txLogItem).
                    withExpressionAttributeNames(ImmutableMapParameter.of("#txId", TransactionLogItemMapper.TRANSACTION_ID_KEY_NAME)).
                    withConditionExpression("attribute_not_exists(#txId)");
        } else {
            return txLogItemMapper.generatePutItemRequest(txLogItem).
                    withExpressionAttributeNames(ImmutableMapParameter.of("#txStatus", TransactionLogItemMapper.TRANSACTION_STATUS_KEY_NAME)).
                    withExpressionAttributeValues(ImmutableMapParameter.of(":txStatus", new AttributeValue(txLogItem.getPreviousTransactionStatus().name()))).
                    withConditionExpression("#txStatus = :txStatus");
        }
    }
}
