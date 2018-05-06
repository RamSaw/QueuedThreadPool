package com.mikhail.pravilov.mit.queuedThreadPool;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.concurrent.*;

/**
 * Implementation of thread pool with balancing queues: if one executor is too busy than others it won't get a task.
 */
public class QueuedThreadPool {
    /**
     * Tasks ({@link FutureTask}) that are not given to one of executors yet.
     */
    @NotNull
    final private BlockingQueue<FutureTask> tasksToSubmit = new LinkedBlockingQueue<>();
    /**
     * Thread that runs {@link ExecutorsDealer}.
     */
    @NotNull
    private Thread executorsDealerThread;
    /**
     * Flag for decision if to accept more tasks for execution or not.
     */
    private boolean isShutdown = false;

    /**
     * Creates {@link QueuedThreadPool} with given number of threads (executors).
     *
     * @param numberOfThreads number of executors.
     * @param maxQueueSize    of executor's queue.
     */
    public QueuedThreadPool(int numberOfThreads, int maxQueueSize) {
        ExecutorsDealer executorsDealer = new ExecutorsDealer(numberOfThreads, maxQueueSize);
        executorsDealerThread = new Thread(executorsDealer);
        executorsDealerThread.start();
    }

    /**
     * Submits new to task, then dealer will give it to the most free executor.
     *
     * @param task to submit.
     * @param <T>  result type of given task execution.
     * @return future for given task.
     */
    public <T> @NotNull Future<T> submit(@NotNull Callable<T> task) {
        if (isShutdown) {
            throw new RejectedExecutionException("Finished accepting tasks");
        }
        FutureTask<T> futureTask = new FutureTask<>(task);
        try {
            tasksToSubmit.put(futureTask);
        } catch (InterruptedException e) {
            throw new RejectedExecutionException("Putting task was interrupted", e);
        }
        return futureTask;
    }

    /**
     * Closes accepting new tasks, all tasks submitted before will be finished.
     */
    public void shutdown() {
        isShutdown = true;
        executorsDealerThread.interrupt();
    }

    /**
     * Implementation of task to deal with tasks that are not given to executor yet.
     */
    private class ExecutorsDealer implements Runnable {
        /**
         * Executors tasks.
         */
        @NotNull
        final private ArrayList<QueuedTaskExecutor> executors = new ArrayList<>();
        /**
         * Threads where executors are running.
         */
        @NotNull
        final private ArrayList<Thread> executorThreads = new ArrayList<>();
        private int maxQueueSize;

        /**
         * Constructs executors dealer task with given numbers of executors.
         *
         * @param numberOfThreads number of executors.
         * @param maxQueueSize    maximum size of executor's queue.
         */
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

        /**
         * Tries to get not submitted task and put into one of the most free executor.
         * If there is no tasks then does nothing.
         * If task is erased from tasksToSubmit then it is guaranteed that it will be given to executor.
         */
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

        /**
         * Returns one of the most free executor.
         * It is not guaranteed that it is the most free executor. To accomplish that is very resources costly.
         * So it is more effectively to get one of the most free.
         *
         * @return executor that has minimum of tasks in the queue.
         */
        @Nullable
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

    /**
     * Executor with it's own queue. Just takes tasks from it and runs.
     * If it is interrupted then finishes all remaining tasks and ends.
     * Invariant: task queue is not updated by outside code since thread was interrupted.
     */
    private class QueuedTaskExecutor implements Runnable {
        /**
         * Tasks of executor.
         */
        @NotNull
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