package com.github.paleblue.persistence.milkha.dto;


import static com.github.paleblue.persistence.milkha.util.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

public class TransactionLogItem {

    private final String transactionId;
    private TransactionStatus transactionStatus;
    private TransactionStatus previousTransactionStatus;
    private Long waitPeriodBeforeSweeperUnlockMillis;
    private Long waitPeriodBeforeSweeperDeleteMillis;
    private boolean unlockedBySweeper;
    private Map<String, List<Map<String, AttributeValue>>> createSet; // Table name -> List of primary keys
    private Map<String, List<Map<String, AttributeValue>>> deleteSet; // Table name -> List of primary keys

    public TransactionLogItem(String transactionId, TransactionStatus transactionStatus, Long waitPeriodBeforeSweeperUnlockMillis, Long waitPeriodBeforeSweeperDeleteMillis) {
        this.transactionId = checkNotNull(transactionId);
        this.transactionStatus = checkNotNull(transactionStatus);
        this.previousTransactionStatus = null;
        this.waitPeriodBeforeSweeperUnlockMillis = waitPeriodBeforeSweeperUnlockMillis;
        this.waitPeriodBeforeSweeperDeleteMillis = waitPeriodBeforeSweeperDeleteMillis;
        this.unlockedBySweeper = false;
        this.createSet = new HashMap<>();
        this.deleteSet = new HashMap<>();
    }

    public TransactionLogItem(String transactionId, TransactionStatus transactionStatus, Long waitPeriodBeforeSweeperUnlockMillis, Long waitPeriodBeforeSweeperDeleteMillis,
            boolean unlockedBySweeper, Map<String, List<Map<String, AttributeValue>>> createSet, Map<String, List<Map<String, AttributeValue>>> deleteSet) {
        this.transactionId = transactionId;
        this.transactionStatus = transactionStatus;
        this.previousTransactionStatus = transactionStatus;
        this.waitPeriodBeforeSweeperUnlockMillis = waitPeriodBeforeSweeperUnlockMillis;
        this.waitPeriodBeforeSweeperDeleteMillis = waitPeriodBeforeSweeperDeleteMillis;
        this.unlockedBySweeper = unlockedBySweeper;
        if (createSet != null) {
            this.createSet = createSet;
        } else {
            this.createSet = Collections.emptyMap();
        }
        if (deleteSet != null) {
            this.deleteSet = deleteSet;
        } else {
            this.deleteSet = Collections.emptyMap();
        }
    }

    public TransactionStatus getTransactionStatus() {
        return transactionStatus;
    }

    public void setTransactionStatus(TransactionStatus transactionStatus) {
        checkNotNull(transactionStatus);
        TransactionStatus.validateStateTransition(this.transactionStatus, transactionStatus);
        this.previousTransactionStatus = this.transactionStatus;
        this.transactionStatus = transactionStatus;
    }

    public TransactionStatus getPreviousTransactionStatus() {
        return previousTransactionStatus;
    }

    public long getWaitPeriodBeforeSweeperUnlockMillis() {
        return waitPeriodBeforeSweeperUnlockMillis;
    }

    public void setWaitPeriodBeforeSweeperUnlockMillis(long waitPeriodBeforeSweeperUnlockMillis) {
        this.waitPeriodBeforeSweeperUnlockMillis = waitPeriodBeforeSweeperUnlockMillis;
    }

    public long getWaitPeriodBeforeSweeperDeleteMillis() {
        return waitPeriodBeforeSweeperDeleteMillis;
    }

    public void setWaitPeriodBeforeSweeperDeleteMillis(long waitPeriodBeforeSweeperDeleteMillis) {
        this.waitPeriodBeforeSweeperDeleteMillis = waitPeriodBeforeSweeperDeleteMillis;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public boolean isUnlockedBySweeper() {
        return unlockedBySweeper;
    }

    public void setUnlockedBySweeper(boolean unlockedBySweeper) {
        this.unlockedBySweeper = unlockedBySweeper;
    }

    public Map<String, List<Map<String, AttributeValue>>> getCreateSet() {
        return createSet;
    }

    public Map<String, List<Map<String, AttributeValue>>> getDeleteSet() {
        return deleteSet;
    }

    public void addToCreateSet(String tableName, Map<String, AttributeValue> key) {
        createSet.putIfAbsent(tableName, new ArrayList<>());
        createSet.get(tableName).add(key);
    }

    public void addToDeleteSet(String tableName, Map<String, AttributeValue> key) {
        deleteSet.putIfAbsent(tableName, new ArrayList<>());
        deleteSet.get(tableName).add(key);
    }
}
