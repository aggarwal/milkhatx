package com.github.paleblue.persistence.milkha.dto;


public class BankAccountItem {

    private String beneficiaryName;
    private Integer totalAmountInUsd;
    private String accountType;

    public BankAccountItem(String beneficiaryName, String accountType, Integer totalAmountInUsd) {
        this.beneficiaryName = beneficiaryName;
        this.totalAmountInUsd = totalAmountInUsd;
        this.accountType = accountType;
    }

    public String getBeneficiaryName() {
        return beneficiaryName;
    }

    public Integer getTotalAmountInUsd() {
        return totalAmountInUsd;
    }

    public String getAccountType() {
        return accountType;
    }
}
