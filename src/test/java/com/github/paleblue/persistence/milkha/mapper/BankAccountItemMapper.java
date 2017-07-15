package com.github.paleblue.persistence.milkha.mapper;


import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.paleblue.persistence.milkha.dto.BankAccountItem;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public class BankAccountItemMapper extends HashAndRangeMapper<BankAccountItem> {

    private static final String BANK_ACCOUNT_ITEM_TABLE_NAME = "BankAccounts";

    public static final String ACCOUNT_BENEFICIARY_KEY_NAME = "accountBeneficiary";
    public static final String TOTAL_AMOUNT_IN_USD_KEY_NAME = "totalAmountInUsd";
    public static final String ACCOUNT_TYPE_KEY_NAME = "accountType";
    public static final String TOTAL_AMOUNT_LSI_NAME = "totalAmountIndex";

    @Override
    public Map<String, AttributeValue> marshall(BankAccountItem item) {
        Map<String, AttributeValue> itemMap = new HashMap<>(3);
        itemMap.put(ACCOUNT_BENEFICIARY_KEY_NAME, new AttributeValue(item.getBeneficiaryName()));
        itemMap.put(TOTAL_AMOUNT_IN_USD_KEY_NAME, new AttributeValue().withN(item.getTotalAmountInUsd().toString()));
        itemMap.put(ACCOUNT_TYPE_KEY_NAME, new AttributeValue(item.getAccountType()));
        return itemMap;
    }

    @Override
    public BankAccountItem unmarshall(Map<String, AttributeValue> itemAttributeMap) {
        String accountBeneficiary = itemAttributeMap.get(ACCOUNT_BENEFICIARY_KEY_NAME).getS();
        Integer totalAmountInUsd = new Integer(itemAttributeMap.get(TOTAL_AMOUNT_IN_USD_KEY_NAME).getN());
        String accountType = itemAttributeMap.get(ACCOUNT_TYPE_KEY_NAME).getS();
        return new BankAccountItem(accountBeneficiary, accountType, totalAmountInUsd);
    }

    @Override
    public String getTableName() {
        return BANK_ACCOUNT_ITEM_TABLE_NAME;
    }

    @Override
    public String getHashKeyName() {
        return ACCOUNT_BENEFICIARY_KEY_NAME;
    }

    @Override
    public String getRangeKeyName() {
        return ACCOUNT_TYPE_KEY_NAME;
    }

    @Override
    public Map<String, AttributeValue> getPrimaryKeyMap(String hashKeyValue) {
        throw new UnsupportedOperationException("This table needs a range key.");
    }

    @Override
    public Map<String, AttributeValue> getPrimaryKeyMap(String hashKeyValue, String rangeKeyValue) {
        Map<String, AttributeValue> primaryKeyMap = new HashMap<>(2);
        primaryKeyMap.put(ACCOUNT_BENEFICIARY_KEY_NAME, new AttributeValue(hashKeyValue));
        primaryKeyMap.put(ACCOUNT_TYPE_KEY_NAME, new AttributeValue(rangeKeyValue));
        return primaryKeyMap;
    }

    @Override
    public List<AttributeDefinition> getAttributeDefinitions() {
        return Arrays.asList(
                new AttributeDefinition(ACCOUNT_BENEFICIARY_KEY_NAME, ScalarAttributeType.S),
                new AttributeDefinition(ACCOUNT_TYPE_KEY_NAME, ScalarAttributeType.S),
                new AttributeDefinition(TOTAL_AMOUNT_IN_USD_KEY_NAME, ScalarAttributeType.N));
    }


    @Override
    public List<LocalSecondaryIndex> getLocalSecondaryIndices() {
        List<KeySchemaElement> indexKeySchema = Arrays.asList(
                new KeySchemaElement().withAttributeName(getHashKeyName()).withKeyType(KeyType.HASH),
                new KeySchemaElement().withAttributeName(TOTAL_AMOUNT_IN_USD_KEY_NAME).withKeyType(KeyType.RANGE));
        Projection projection = new Projection().withProjectionType(ProjectionType.INCLUDE);
        projection.setNonKeyAttributes(Arrays.asList(ACCOUNT_TYPE_KEY_NAME));
        LocalSecondaryIndex localSecondaryIndex = new LocalSecondaryIndex().withIndexName(TOTAL_AMOUNT_LSI_NAME).
                withKeySchema(indexKeySchema).withProjection(projection);
        return Arrays.asList(localSecondaryIndex);
    }
}
