package com.github.paleblue.persistence.milkha.dto;


import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.paleblue.persistence.milkha.exception.InvalidTransactionStateTransitionException;

public enum TransactionStatus {
    NOT_PERSISTED, // not persisted to DDB
    START_COMMIT,
    COMMITTED,
    ROLLED_BACK,
    COMPLETE; // not persisted to DDB

    private static Map<TransactionStatus, List<TransactionStatus>> validTransitions = new HashMap<>();

    static {
        validTransitions.put(NOT_PERSISTED, Arrays.asList(START_COMMIT, COMPLETE));
        validTransitions.put(START_COMMIT, Arrays.asList(COMMITTED, ROLLED_BACK));
        validTransitions.put(COMMITTED, Arrays.asList(COMPLETE, START_COMMIT));
        validTransitions.put(ROLLED_BACK, Arrays.asList(COMPLETE, ROLLED_BACK));
        validTransitions.put(COMPLETE, Arrays.asList(COMPLETE));
    }

    public static void validateStateTransition(TransactionStatus from, TransactionStatus to) {
        if (from == null || !validTransitions.containsKey(from) || !validTransitions.get(from).contains(to)) {
            throw new InvalidTransactionStateTransitionException(String.format("Transition from %s to %s is not allowed",
                    String.valueOf(from), String.valueOf(to)));
        }
    }

}
