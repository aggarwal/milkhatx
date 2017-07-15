package com.github.paleblue.persistence.milkha.mapper;


import static org.junit.Assert.assertEquals;

import java.util.Map;

import com.github.paleblue.persistence.milkha.dto.BankAccountItem;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.junit.Before;
import org.junit.Test;

public class BankAccountItemMapperTest {

    private BankAccountItemMapper bankAccountItemMapper;

    @Before
    public void setup() {
        bankAccountItemMapper = new BankAccountItemMapper();
    }

    @Test
    public void marshallAndUnmarshallYieldsSameObject() {
        String beneficiaryName = "DrEvil";
        Integer totalAmountInUsd = 1000000;
        String accountType = "savings";
        BankAccountItem originalItem = new BankAccountItem(beneficiaryName, accountType, totalAmountInUsd);
        Map<String, AttributeValue> marshalledItem = bankAccountItemMapper.marshall(originalItem);
        BankAccountItem unmarshalledItem = bankAccountItemMapper.unmarshall(marshalledItem);
        assertEquals(beneficiaryName, unmarshalledItem.getBeneficiaryName());
        assertEquals(totalAmountInUsd, unmarshalledItem.getTotalAmountInUsd());
    }
}
