package com.mikhail.pravilov.mit.queuedThreadPool;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.concurrent.*;

public class QueuedThreadPool {
    final private BlockingQueue<FutureTask> tasksToSubmit = new LinkedBlockingQueue<>();
    private Thread executorsDealerThread;
    private boolean isShutdown = false;

    public QueuedThreadPool(int numberOfThreads, int maxQueueSize) {
        ExecutorsDealer executorsDealer = new ExecutorsDealer(numberOfThreads, maxQueueSize);
        executorsDealerThread = new Thread(executorsDealer);
        executorsDealerThread.start();
    }

    public <T> Future<T> submit(@NotNull Callable<T> task) throws InterruptedException {
        if (isShutdown) {
            throw new RejectedExecutionException("Finished accepting tasks");
        }
        FutureTask<T> futureTask = new FutureTask<>(task);
        tasksToSubmit.put(futureTask);
        return futureTask;
    }

    public void shutdown() {
        isShutdown = true;
        executorsDealerThread.interrupt();
    }

    private class ExecutorsDealer implements Runnable {
        final private ArrayList<QueuedTaskExecutor> executors = new ArrayList<>();
        final private ArrayList<Thread> executorThreads = new ArrayList<>();
        private int maxQueueSize;

        ExecutorsDealer(int numberOfThreads, int maxQueueSize) {
            this.maxQueueSize = maxQueueSize;
            QueuedTaskExecutor queuedTaskExecutor = new QueuedTaskExecutor();
            for (int i = 0; i < numberOfThreads; i++) {
                executors.add(queuedTaskExecutor);
                executorThreads.add(new Thread(queuedTaskExecutor));
                executorThreads.get(executorThreads.size() - 1).start();
            }
        }

        @Override
        public void run() {
            while (!Thread.interrupted()) {
                submitTaskToExecutor();
            }
            while (!tasksToSubmit.isEmpty()) {
                submitTaskToExecutor();
            }
            for (Thread executorThread : executorThreads) {
                executorThread.interrupt();
            }
        }

        private void submitTaskToExecutor() {
            QueuedTaskExecutor minLoadedExecutor = getMinLoadedExecutor();
            if (minLoadedExecutor != null) {
                FutureTask task = null;
                boolean setInterrupted = false;
                try {
                    task = tasksToSubmit.take();
                } catch (InterruptedException ignored) {
                    setInterrupted = true;
                }
                while (task != null) {
                    try {
                        minLoadedExecutor.tasks.put(task);
                    } catch (InterruptedException e) {
                        setInterrupted = true;
                        continue;
                    }
                    task = null;
                }
                if (setInterrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        private QueuedTaskExecutor getMinLoadedExecutor() {
            QueuedTaskExecutor minLoadedExecutor = null;
            int minSize = Integer.MAX_VALUE;
            for (QueuedTaskExecutor executor : executors) {
                int size = executor.tasks.size();
                if (size < maxQueueSize && size < minSize) {
                    minSize = size;
                    minLoadedExecutor = executor;
                }
            }
            return minLoadedExecutor;
        }
    }

    private class QueuedTaskExecutor implements Runnable {
        private final BlockingQueue<FutureTask> tasks = new LinkedBlockingQueue<>();

        @Override
        public void run() {
            FutureTask task;
            while (!Thread.interrupted()) {
                try {
                    task = tasks.take();
                    task.run();
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
            while (!tasks.isEmpty()) {
                try {
                    task = tasks.take();
                    task.run();
                } catch (InterruptedException ignored) {
                }
            }
        }
    }
}