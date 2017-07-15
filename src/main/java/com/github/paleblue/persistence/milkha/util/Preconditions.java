package com.github.paleblue.persistence.milkha.util;


public final class Preconditions {

    private Preconditions() {
    }

    public static <T> T checkNotNull(T tableName) {
        if (null == tableName) {
            throw new NullPointerException();
        } else {
            return tableName;
        }
    }

    public static void checkArgument(boolean expression, String errorMessage) {
        if (!expression) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

}
