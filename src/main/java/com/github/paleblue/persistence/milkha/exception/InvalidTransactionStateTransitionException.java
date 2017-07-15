package com.github.paleblue.persistence.milkha.exception;


public class InvalidTransactionStateTransitionException extends RuntimeException {
    public InvalidTransactionStateTransitionException() {
        super();
    }

    public InvalidTransactionStateTransitionException(String message) {
        super(message);
    }

    public InvalidTransactionStateTransitionException(String message, Throwable cause) {
        super(message, cause);
    }
}
