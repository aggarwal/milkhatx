package com.github.paleblue.persistence.milkha.util;


import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.github.paleblue.persistence.milkha.exception.TransactionTimedOutException;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;

public final class Futures {

    private static final Log LOG = LogFactory.getLog(Futures.class);

    private Futures() {
    }


    public static void blockOnAllFutures(List<Future> futures, Instant endTime) {
        blockOnAllFutures(futures, endTime, null);
    }

    /**
     *
     * @param futures list of futures on which we need to block on
     * @param endTime a time instant in the future which when reached before all futures are complete, will throw TransactionTimedOutException
     */
    public static void blockOnAllFutures(List<Future> futures, Instant endTime, RuntimeException exception) {
        try {
            for (Future future : futures) {
                long remainingTime = Instant.now().until(endTime, ChronoUnit.MILLIS);
                future.get(remainingTime, TimeUnit.MILLISECONDS);
            }
        } catch (TimeoutException e) {
            futures.forEach(f -> f.cancel(true));
            throw new TransactionTimedOutException("Transaction timed out", e);
        } catch (InterruptedException | ExecutionException | CancellationException e) {
            LOG.warn(e);
            Throwable dynamoException = e.getCause();
            if ((dynamoException instanceof ConditionalCheckFailedException) && exception != null) {
                throw exception;
            } else if (dynamoException instanceof ProvisionedThroughputExceededException) {
                throw new ProvisionedThroughputExceededException(dynamoException.getMessage());
            } else {
                throw new RuntimeException(dynamoException);
            }
        }
    }

}
