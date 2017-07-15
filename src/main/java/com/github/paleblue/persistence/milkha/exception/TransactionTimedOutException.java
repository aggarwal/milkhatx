package com.github.paleblue.persistence.milkha.exception;

public class TransactionTimedOutException extends RuntimeException {
    public TransactionTimedOutException() {
        super();
    }

    public TransactionTimedOutException(String message) {
        super(message);
    }

    public TransactionTimedOutException(String message, Throwable cause) {
        super(message, cause);
    }
}
