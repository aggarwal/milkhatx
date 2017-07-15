package com.github.paleblue.persistence.milkha.exception;


public class TransactionPendingException extends RuntimeException {
    public TransactionPendingException() {
        super();
    }

    public TransactionPendingException(String message) {
        super(message);
    }

    public TransactionPendingException(String message, Throwable cause) {
        super(message, cause);
    }
}
