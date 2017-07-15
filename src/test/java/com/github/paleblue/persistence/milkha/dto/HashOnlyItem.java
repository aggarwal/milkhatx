package com.github.paleblue.persistence.milkha.dto;


public class HashOnlyItem {
    String hashKey;

    public HashOnlyItem(String hashKey) {
        this.hashKey = hashKey;
    }

    public String getHashKey() {
        return hashKey;
    }
}
