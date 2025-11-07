/*
 * Copyright (C) 2025, exense GmbH
 *
 * This file is part of Step
 *
 * Step is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Step is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Step.  If not, see <http://www.gnu.org/licenses/>.
 */

package step.streaming.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A generic batching processor that accumulates items and processes them in batches
 * based on size and time thresholds.
 *
 * @param <T> the type of items to batch
 */
public class BatchProcessor<T> implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(BatchProcessor.class);

    private final Function<List<T>, Boolean> shouldFlushPredicate;
    private final long flushIntervalMs;
    private final Consumer<List<T>> batchProcessor;
    private final String processorName;

    // explicit lock object replaced by synchronization on batch itself
    private final List<T> batch = new ArrayList<>();
    private final ScheduledExecutorService scheduler;
    private volatile long lastFlushTime;

    /**
     * Creates a new BatchProcessor with the specified configuration.
     *
     * @param batchSize       the maximal number of items to batch before processing immediately
     * @param flushIntervalMs the maximum time in milliseconds to wait before processing
     * @param batchProcessor  the function to process batches of items
     * @param processorName   a name for this processor (used for logging and thread naming)
     */
    public BatchProcessor(int batchSize, long flushIntervalMs, Consumer<List<T>> batchProcessor, String processorName) {
        this(queued -> queued.size() >= batchSize, flushIntervalMs, batchProcessor, processorName);
    }

    /**
     * Creates a new BatchProcessor with the specified configuration.
     *
     * @param shouldFlashPredicate a function determining whether to flush immediately given the currently batched items.
     * @param flushIntervalMs      the maximum time in milliseconds to wait before processing
     * @param batchProcessor       the function to process batches of items
     * @param processorName        a name for this processor (used for logging and thread naming)
     */
    public BatchProcessor(Function<List<T>, Boolean> shouldFlashPredicate, long flushIntervalMs, Consumer<List<T>> batchProcessor, String processorName) {
        this.shouldFlushPredicate = Objects.requireNonNull(shouldFlashPredicate);
        if (flushIntervalMs <= 0) throw new IllegalArgumentException("flushIntervalMs must be positive");
        this.flushIntervalMs = flushIntervalMs;
        this.batchProcessor = Objects.requireNonNull(batchProcessor);
        this.processorName = processorName != null ? processorName : "batch-processor";

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, this.processorName + "-flusher");
            t.setDaemon(true);
            return t;
        });
        this.lastFlushTime = System.currentTimeMillis();

        this.scheduler.scheduleAtFixedRate(this::flushIfNeeded, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Adds an item to the batch. If the batch reaches the configured size,
     * it will be processed immediately.
     *
     * @param item the item to add to the batch
     * @return the number of batched items that will be processed later, or 0 if items were directly processed during the invocation
     */
    public int add(T item) {
        synchronized (batch) {
            batch.add(item);
            if (shouldFlushPredicate.apply(batch)) {
                flushBatch();
                return 0;
            } else {
                return batch.size();
            }
        }
    }

    /**
     * Forces processing of any items currently in the batch, regardless of size or time thresholds.
     */
    public void flush() {
        flushBatch();
    }

    /**
     * Gets the current number of items in the batch waiting to be processed.
     *
     * @return the current batch size
     */
    public int getCurrentBatchSize() {
        synchronized (batch) {
            return batch.size();
        }
    }

    private void flushIfNeeded() {
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastFlushTime >= flushIntervalMs) {
            flushBatch();
        }
    }

    private void flushBatch() {
        // Note: this currently keeps the lock while processing (as in the original implementation).
        // An alternative implementation could process AFTER releasing the lock, but that feels less safe.
        synchronized (batch) {
            if (batch.isEmpty()) {
                return;
            }
            List<T> toProcess = new ArrayList<>(batch);
            batch.clear();
            lastFlushTime = System.currentTimeMillis();

            try {
                batchProcessor.accept(toProcess);
                logger.debug("Successfully processed batch of {} items on {}", toProcess.size(), processorName);
            } catch (Exception e) {
                logger.error("Failed to process batch of {} items on {}", toProcess.size(), processorName, e);
                throw e;
            }
        }
    }

    @Override
    public void close() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        flushBatch();
    }
}