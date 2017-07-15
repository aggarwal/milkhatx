package com.github.paleblue.persistence.milkha.exception;


public class ContentionException extends RuntimeException {
    public ContentionException() {
        super();
    }

    public ContentionException(String message) {
        super(message);
    }

    public ContentionException(String message, Throwable cause) {
        super(message, cause);
    }
}
