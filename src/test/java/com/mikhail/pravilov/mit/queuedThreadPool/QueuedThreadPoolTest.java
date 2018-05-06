package com.mikhail.pravilov.mit.queuedThreadPool;

import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import static org.junit.Assert.*;

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
    public void shutdownThrowsException() {
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
        for (int i = 0; i < 500; i++) {
            int finalI = i;
            tasks.add(queuedThreadPool.submit(() -> {
                for (int j = 0; j < 100; j++) {
                    j++;
                }
                return finalI * 10;
            }));
        }
        queuedThreadPool.shutdown();
        for (int i = 0; i < 500; i++) {
            assertEquals(new Integer(i * 10), tasks.get(i).get());
        }
    }
}