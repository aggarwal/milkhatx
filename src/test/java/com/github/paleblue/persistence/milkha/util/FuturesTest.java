package com.github.paleblue.persistence.milkha.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.http.exception.HttpRequestTimeoutException;
import com.github.paleblue.persistence.milkha.exception.ContentionException;
import com.github.paleblue.persistence.milkha.exception.TransactionTimedOutException;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;

@RunWith(MockitoJUnitRunner.class)
public class FuturesTest {

    @Mock Future future;

    ExecutorService executorService;

    @Before
    public void init() {
        executorService = Executors.newSingleThreadExecutor();
    }

    @Test(expected = TransactionTimedOutException.class)
    public void whenFutureThrowTimeoutExceptionThenFuturesThrowTransactionTimedOutException() throws Exception {
        doThrow(TimeoutException.class).when(future).get(anyLong(), any(TimeUnit.class));
        Futures.blockOnAllFutures(Arrays.asList(future), Instant.now().plusSeconds(1));
    }

    @Test(expected = TransactionTimedOutException.class)
    public void whenANegativeTimeoutIsSpecifiedThenAFutureWillTimeoutImmediately() throws Exception {
        doThrow(TimeoutException.class).when(future).get(anyLong(), any(TimeUnit.class));
        Futures.blockOnAllFutures(Arrays.asList(future), Instant.now().minusSeconds(1));
    }

    @Test(expected = ContentionException.class)
    public void whenAFutureFailsDueToConditionCheckFailedExceptionThenUserSpecifiedExceptionIsThrown() throws Exception {
        doThrow(new ExecutionException( new ConditionalCheckFailedException("condition check failed exception")))
                .when(future).get(anyLong(), any(TimeUnit.class));
        Futures.blockOnAllFutures(Arrays.asList(future), Instant.now().plusSeconds(1), new ContentionException());
    }

    @Test(expected = RuntimeException.class)
    public void whenFutureFailsDueToOtherExceptionThenRuntimeExceptionIsThrowns() throws Exception {
        doThrow(new ExecutionException( new HttpRequestTimeoutException("some other exception")))
                .when(future).get(anyLong(), any(TimeUnit.class));
        Futures.blockOnAllFutures(Arrays.asList(future), Instant.now().plusSeconds(1), new ContentionException());
    }

    @Test
    public void whenAllFuturesAreSuccessfulBeforeTimeoutNoExceptionIsThrown() throws Exception {
        Future future = constructTestFuture(1);
        Futures.blockOnAllFutures(Arrays.asList(future), Instant.now().plusMillis(4000L));
        future.get(); // making sure that future completed without any exception
    }

    @Test
    public void whenTimeoutHappensThenFuturesThatDidntCompleteAreCancelled() throws Exception {
        List<Future> futures = new ArrayList<>();
        IntStream.range(0, 2).forEach((i -> futures.add(constructTestFuture(i))));
        try {
            Futures.blockOnAllFutures(futures, Instant.now().plusMillis(500L));
        } catch (TransactionTimedOutException e) {
            assertFalse(futures.get(0).isCancelled());
            assertTrue(futures.get(1).isCancelled());
        }
    }

    private Future constructTestFuture(final int counter) {
        return executorService.submit(() -> {
            try {
                Thread.sleep(counter * 1000L);
                return;
            } catch (InterruptedException ignore) {
            }
        });
    }

}
