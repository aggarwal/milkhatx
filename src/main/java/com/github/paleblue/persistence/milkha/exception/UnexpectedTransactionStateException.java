package com.github.paleblue.persistence.milkha.exception;


public class UnexpectedTransactionStateException extends RuntimeException {
    public UnexpectedTransactionStateException() {
        super();
    }

    public UnexpectedTransactionStateException(String message) {
        super(message);
    }

    public UnexpectedTransactionStateException(String message, Throwable cause) {
        super(message, cause);
    }
}
