package com.github.paleblue.persistence.milkha.dto;


public class HashAndRangeOnlyItem {

    private String hashKey;
    private String rangeKey;

    public HashAndRangeOnlyItem(String hashKey, String rangeKey) {
        this.hashKey = hashKey;
        this.rangeKey = rangeKey;
    }

    public String getHashKey() {
        return hashKey;
    }

    public String getRangeKey() {
        return rangeKey;
    }
}
