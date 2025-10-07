package step.streaming.websocket.test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadPools {
    public static int getDefaultPoolSize() {
        return Math.max(4, Runtime.getRuntime().availableProcessors());
    }

    public static ExecutorService createPoolExecutor(String threadPrefix) {
        return createPoolExecutor(threadPrefix, getDefaultPoolSize());
    }

    public static ExecutorService createPoolExecutor(String threadPrefix, int poolSize) {
        return createPoolExecutor(threadPrefix, poolSize, 256);
    }

    public static ExecutorService createPoolExecutor(String threadPrefix, int poolSize, int queueCapacity) {
        return new ThreadPoolExecutor(
                poolSize,
                poolSize,
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                namedDaemon(threadPrefix)
        );
    }

    public static ThreadFactory namedDaemon(String prefix) {
        return new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, prefix + "-" + threadNumber.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        };
    }
}
