package com.mikhail.pravilov.mit.queuedThreadPool;

import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;

public class QueuedThreadPoolTest {
    @Test
    public void submitBasicTest() throws ExecutionException, InterruptedException {
        QueuedThreadPool queuedThreadPool = new QueuedThreadPool(4, 4);
        ArrayList<Future<Integer>> tasks = new ArrayList<>();
        for (int i = 0; i < 160; i++) {
            int finalI = i;
            tasks.add(queuedThreadPool.submit(() -> finalI * 10));
        }
        for (int i = 0; i < 160; i++) {
            assertEquals(new Integer(i * 10), tasks.get(i).get());
        }
    }

    @Test(expected = RejectedExecutionException.class)
    public void shutdownThrowsException() throws InterruptedException {
        QueuedThreadPool queuedThreadPool = new QueuedThreadPool(4, 4);
        for (int i = 0; i < 160; i++) {
            queuedThreadPool.submit(() -> 10);
            if (i > 100) {
                queuedThreadPool.shutdown();
            }
        }
    }

    @Test
    public void shutdownCheckValuesCalculated() throws ExecutionException, InterruptedException {
        QueuedThreadPool queuedThreadPool = new QueuedThreadPool(4, 4);
        ArrayList<Future<Integer>> tasks = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            int finalI = i;
            tasks.add(queuedThreadPool.submit(() -> finalI * 10));
        }
        queuedThreadPool.shutdown();
        for (int i = 0; i < 5000; i++) {
            assertEquals(new Integer(i * 10), tasks.get(i).get());
        }
    }

    @Test
    public void check1ThreadWithOneSizedQueue() throws ExecutionException, InterruptedException {
        checkWithDifferentTasks(1, 1);
    }

    @Test
    public void check4ThreadsWith1SizedQueue() throws ExecutionException, InterruptedException {
        checkWithDifferentTasks(4, 1);
    }

    @Test
    public void check1ThreadWith20SizedQueue() throws ExecutionException, InterruptedException {
        checkWithDifferentTasks(1, 20);
    }

    @Test
    public void check4ThreadsWith20SizedQueue() throws ExecutionException, InterruptedException {
        checkWithDifferentTasks(4, 20);
    }

    private void checkWithDifferentTasks(int numberOfThreads, int maxQueueSize) throws ExecutionException, InterruptedException {
        // Few Easy Tasks
        checkThreadPoolWithTasks(new QueuedThreadPool(numberOfThreads, maxQueueSize), 10, 10);
        // Few Heavy Tasks
        checkThreadPoolWithTasks(new QueuedThreadPool(numberOfThreads, maxQueueSize), 10, 10000);
        // Many Easy Tasks
        checkThreadPoolWithTasks(new QueuedThreadPool(numberOfThreads, maxQueueSize), 10000, 10);
        // Many Heavy Tasks
        checkThreadPoolWithTasks(new QueuedThreadPool(numberOfThreads, maxQueueSize), 10000, 10000);
    }

    private void checkThreadPoolWithTasks(QueuedThreadPool queuedThreadPool, int numberOfTasks, int executionTimes)
            throws ExecutionException, InterruptedException {
        ArrayList<Future<Integer>> tasks = new ArrayList<>();
        for (int i = 0; i < numberOfTasks; i++) {
            int finalI = i;
            tasks.add(queuedThreadPool.submit(() -> {
                // Random is used to avoid code JIT optimizations.
                for (int j = 0; j < executionTimes; j++) {
                    int a = ThreadLocalRandom.current().nextInt(0, 10000);
                    if (a % 2 == 0) {
                        j++;
                    }
                }
                return finalI;
            }));
        }
        queuedThreadPool.shutdown();
        for (int i = 0; i < numberOfTasks; i++) {
            assertEquals(new Integer(i), tasks.get(i).get());
        }
    }
}