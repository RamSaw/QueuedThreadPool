package com.mikhail.pravilov.mit.queuedThreadPool;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.concurrent.*;

public class QueuedThreadPool {
    final private ArrayList<QueuedTaskExecutor> threadTasks = new ArrayList<>();
    final private ArrayList<Thread> threads = new ArrayList<>();
    final private BlockingQueue<FutureTask> tasksToSubmit = new LinkedBlockingQueue<>();
    private Thread executorsDealerThread;
    private boolean isShutdown = false;
    private int maxQueueSize;

    public QueuedThreadPool(int numberOfThreads, int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
        QueuedTaskExecutor queuedTaskExecutor = new QueuedTaskExecutor();
        for (int i = 0; i < numberOfThreads; i++) {
            threadTasks.add(queuedTaskExecutor);
            threads.add(new Thread(queuedTaskExecutor));
            threads.get(threads.size() - 1).start();
        }
        executorsDealerThread = new Thread(new ExecutorsDealer());
        executorsDealerThread.start();
    }

    public <T> Future<T> submit(@NotNull Callable<T> task) {
        if (isShutdown) {
            throw new RejectedExecutionException("Finished accepting tasks");
        }
        FutureTask<T> futureTask = new FutureTask<>(task);
        Runnable putTask = () -> {
            try {
                tasksToSubmit.put(futureTask);
            } catch (InterruptedException ignored) {
            }
        };
        (new Thread(putTask)).start();
        return futureTask;
    }

    public void shutdown() {
        isShutdown = true;
        executorsDealerThread.interrupt();
    }

    private class ExecutorsDealer implements Runnable {
        @Override
        public void run() {
            while (!Thread.interrupted()) {
                QueuedTaskExecutor minLoadedExecutor = getMinLoadedExecutor();
                if (minLoadedExecutor != null) {
                    try {
                        minLoadedExecutor.addTask(tasksToSubmit.take());
                    } catch (InterruptedException ignored) {
                    }
                }
            }
            for (Thread thread : threads) {
                thread.interrupt();
            }
            if (isShutdown) {
                while (!tasksToSubmit.isEmpty()) {
                    QueuedTaskExecutor minLoadedExecutor = getMinLoadedExecutor();
                    if (minLoadedExecutor != null) {
                        try {
                            minLoadedExecutor.addTask(tasksToSubmit.take());
                        } catch (InterruptedException ignored) {
                        }
                    }
                }
            }
        }
    }

    private QueuedTaskExecutor getMinLoadedExecutor() {
        QueuedTaskExecutor minLoadedExecutor = null;
        int minSize = Integer.MAX_VALUE;
        for (QueuedTaskExecutor task : threadTasks) {
            int size = task.getQueueSize();
            if (size < maxQueueSize && size < minSize) {
                minSize = size;
                minLoadedExecutor = task;
            }
        }
        return minLoadedExecutor;
    }

    private class QueuedTaskExecutor implements Runnable {
        private final BlockingQueue<FutureTask> tasks = new LinkedBlockingQueue<>();

        @Override
        public void run() {
            while (!Thread.interrupted()) {
                try {
                    tasks.take().run();
                } catch (InterruptedException ignored) {
                }
            }
            if (isShutdown) {
                while (!tasks.isEmpty()) {
                    try {
                        tasks.take().run();
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        }

        int getQueueSize() {
            return tasks.size();
        }

        void addTask(FutureTask task) throws InterruptedException {
            tasks.put(task);
        }
    }
}