package com.github.paleblue.persistence.milkha;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import com.github.paleblue.persistence.milkha.TransactionSweeper;
import com.github.paleblue.persistence.milkha.TransactionSweeperBuilder;
import com.github.paleblue.persistence.milkha.TransactionSweeperTask;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.github.paleblue.persistence.milkha.dto.TransactionStatus;
import com.github.paleblue.persistence.milkha.mapper.TransactionLogItemMapper;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

public class TransactionSweeperTest {

    @Mock
    AmazonDynamoDB mockDDBClient;

    @Mock
    TransactionSweeperTask txSweeperTask1;

    @Mock
     TransactionSweeperTask txSweeperTask2;

    TransactionSweeperBuilder txSweeperBuilder;
    TransactionSweeper txSweeper;

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
        txSweeperBuilder = new TransactionSweeperBuilder(mockDDBClient, Executors.newSingleThreadScheduledExecutor());
        txSweeper = txSweeperBuilder.build();
    }

    @Test
    public void testExecuteTxSweeperTasksCatchesThrownException() throws Exception {
        when(txSweeperTask1.execute()).thenThrow(Exception.class);
        txSweeper.getSeenTransactions().put(UUID.randomUUID().toString(), txSweeperTask1);
        txSweeper.executeTxSweeperTasks();
    }

    @Test
    public void testScanTxLogTableCatchesThrownException() throws Exception {
        when(mockDDBClient.scan(any(ScanRequest.class))).thenThrow(Exception.class);
        txSweeper.scanTxLogTable();
    }

    @Test
    public void testTransactionSweeperDeletesCompletedTxSweeperTasks() throws Exception {
        when(txSweeperTask1.execute()).thenReturn(false);
        when(txSweeperTask2.execute()).thenReturn(true);
        String transactionId1 = UUID.randomUUID().toString();
        String transactionId2 = UUID.randomUUID().toString();
        when(txSweeperTask1.getTransactionId()).thenReturn(transactionId1);
        when(txSweeperTask2.getTransactionId()).thenReturn(transactionId2);
        txSweeper.getSeenTransactions().put(transactionId1, txSweeperTask1);
        txSweeper.getSeenTransactions().put(transactionId2, txSweeperTask2);
        txSweeper.executeTxSweeperTasks();
        assertEquals(1, txSweeper.getSeenTransactions().size());
        assertEquals(txSweeperTask1, txSweeper.getSeenTransactions().get(transactionId1));
    }

    @Test
    public void testTransactionSweeperDedupesSeenTransactions() throws Exception {
        List<Map<String, AttributeValue>> scanItems1 = generateRawTxLogItems(5);
        List<Map<String, AttributeValue>> scanItems2 = generateRawTxLogItems(2);
        scanItems2.addAll(scanItems1.subList(0, 2));
        ScanResult scanResult1 = new ScanResult()
                .withCount(scanItems1.size())
                .withItems(scanItems1)
                .withLastEvaluatedKey(null);
        ScanResult scanResult2 = new ScanResult()
                .withCount(scanItems2.size())
                .withItems(scanItems2)
                .withLastEvaluatedKey(null);
        when(mockDDBClient.scan(any(ScanRequest.class)))
                .thenReturn(scanResult1)
                .thenReturn(scanResult2);
        txSweeper.scanTxLogTable();
        assertEquals(5, txSweeper.getSeenTransactions().size());
        txSweeper.scanTxLogTable();
        assertEquals(7, txSweeper.getSeenTransactions().size());
    }

    private List<Map<String, AttributeValue>> generateRawTxLogItems(final int numberOfItems) {
        List<Map<String, AttributeValue>> rawTxLogItems = new ArrayList<>();
        IntStream.range(0, numberOfItems).forEach(i -> rawTxLogItems.add(generateNewRawTxLogItem()));
        return rawTxLogItems;
    }

    private Map<String, AttributeValue> generateNewRawTxLogItem() {
        Map<java.lang.String, com.amazonaws.services.dynamodbv2.model.AttributeValue> rawTxLogItem = new HashMap<>();
        rawTxLogItem.put(TransactionLogItemMapper.TRANSACTION_ID_KEY_NAME, new com.amazonaws.services.dynamodbv2.model.AttributeValue().withS(UUID.randomUUID().toString()));
        rawTxLogItem.put(TransactionLogItemMapper.TRANSACTION_STATUS_KEY_NAME, new com.amazonaws.services.dynamodbv2.model.AttributeValue().withS(TransactionStatus.START_COMMIT.name()));
        rawTxLogItem.put(TransactionLogItemMapper.UNLOCKED_BY_SWEEPER, new com.amazonaws.services.dynamodbv2.model.AttributeValue().withBOOL(false));
        rawTxLogItem.put(TransactionLogItemMapper.WAIT_PERIOD_BEFORE_SWEEPER_UNLOCK_MILLIS, new com.amazonaws.services.dynamodbv2.model.AttributeValue().withN("3000"));
        rawTxLogItem.put(TransactionLogItemMapper.WAIT_PERIOD_BEFORE_SWEEPER_DELETE_MILLIS, new com.amazonaws.services.dynamodbv2.model.AttributeValue().withN("6000"));
        return rawTxLogItem;
    }
}
