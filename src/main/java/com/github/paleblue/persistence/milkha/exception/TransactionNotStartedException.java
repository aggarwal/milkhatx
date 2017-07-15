package com.github.paleblue.persistence.milkha.exception;


public class TransactionNotStartedException extends RuntimeException {
    public TransactionNotStartedException() {
        super();
    }

    public TransactionNotStartedException(String message) {
        super(message);
    }

    public TransactionNotStartedException(String message, Throwable cause) {
        super(message, cause);
    }
}
