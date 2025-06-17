import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

interface CustomExecutor extends Executor {
    void execute(Runnable command);
    <T> Future<T> submit(Callable<T> callable);
    void shutdown();
    void shutdownNow();
}

public class CustomThreadPool implements CustomExecutor {

    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTimeMillis;
    private final int queueSize;
    private final int minSpareThreads;

    private final CustomThreadFactory threadFactory;

    private final List<Worker> workers = new ArrayList<>();
    private final List<BlockingQueue<Runnable>> queues = new ArrayList<>();

    private final AtomicInteger rrIndex = new AtomicInteger(0);

    private volatile boolean isShutdown = false;
    private volatile boolean isTerminated = false;

    private final Object mainLock = new Object();

    private final AtomicInteger currentPoolSize = new AtomicInteger(0);

    public CustomThreadPool(int corePoolSize, int maxPoolSize, long keepAliveTime, TimeUnit timeUnit,
                            int queueSize, int minSpareThreads) {
        if (corePoolSize <= 0 || maxPoolSize < corePoolSize || queueSize <= 0 || minSpareThreads < 0)
            throw new IllegalArgumentException("Invalid pool parameters");
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTimeMillis = timeUnit.toMillis(keepAliveTime);
        this.queueSize = queueSize;
        this.minSpareThreads = minSpareThreads;

        this.threadFactory = new CustomThreadFactory("MyPool-worker-");

        for (int i = 0; i < corePoolSize; i++) {
            BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(queueSize);
            queues.add(queue);
            Worker w = new Worker(queue);
            workers.add(w);
            w.start();
            currentPoolSize.incrementAndGet();
        }
    }

    public interface CustomExecutor extends Executor {
        void execute(Runnable command);
        <T> Future<T> submit(Callable<T> callable);
        void shutdown();
        void shutdownNow();
    }

    private class CustomThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        CustomThreadFactory(String prefix) {
            this.namePrefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            String name = namePrefix + threadNumber.getAndIncrement();
            Thread t = new Thread(r, name);
            log("[ThreadFactory] Creating new thread: " + name);
            t.setUncaughtExceptionHandler((thread, ex) -> {
                log("[ThreadFactory] Uncaught exception in thread " + thread.getName() + ": " + ex);
            });
            return t;
        }
    }

    private class Worker extends Thread {
        private final BlockingQueue<Runnable> taskQueue;
        private volatile boolean running = true;

        Worker(BlockingQueue<Runnable> queue) {
            super();
            this.taskQueue = queue;
            Thread t = threadFactory.newThread(this);
            this.setName(t.getName());
            this.setUncaughtExceptionHandler(t.getUncaughtExceptionHandler());
        }

        @Override
        public void run() {
            try {
                while (running) {
                    Runnable task = null;
                    try {
                        task = taskQueue.poll(keepAliveTimeMillis, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException ignored) {
                    }

                    if (task != null) {
                        if (isShutdown && isTerminated) break;
                        log("[Worker] " + getName() + " executes " + taskDescription(task));
                        try {
                            task.run();
                        } catch (Throwable t) {
                            log("[Worker] Exception in task: " + t);
                        }
                    } else {
                        synchronized (mainLock) {
                            if (currentPoolSize.get() > corePoolSize) {
                                log("[Worker] " + getName() + " idle timeout, stopping.");
                                running = false;
                                break;
                            }
                        }
                    }
                }
            } finally {
                log("[Worker] " + getName() + " terminated.");
                currentPoolSize.decrementAndGet();
                synchronized (mainLock) {
                    workers.remove(this);
                    queues.remove(taskQueue);
                    mainLock.notifyAll();
                }
            }
        }

        void shutdown() {
            running = false;
            this.interrupt();
        }
    }

    private String taskDescription(Runnable task) {
        if (task instanceof FutureTask) {
            return "FutureTask";
        } else {
            return task.toString();
        }
    }

    public void execute(Runnable command) {
        if (command == null) throw new NullPointerException("Task is null");
        synchronized (mainLock) {
            if (isShutdown) {
                log("[Rejected] Task " + taskDescription(command) + " was rejected due to shutdown!");
                throw new RejectedExecutionException("Pool is shutdown");
            }
            boolean taskAdded = false;
            int attempts = 0;
            int queuesCount = queues.size();

            while (attempts < queuesCount) {
                int idx = rrIndex.getAndIncrement() % queuesCount;
                BlockingQueue<Runnable> q = queues.get(idx);
                if (q.offer(command)) {
                    log("[Pool] Task accepted into queue #" + idx + ": " + taskDescription(command));
                    taskAdded = true;
                    break;
                }
                attempts++;
            }

            if (!taskAdded) {
                if (currentPoolSize.get() < maxPoolSize) {
                    BlockingQueue<Runnable> newQueue = new ArrayBlockingQueue<>(queueSize);
                    newQueue.offer(command);
                    queues.add(newQueue);
                    Worker w = new Worker(newQueue);
                    workers.add(w);
                    currentPoolSize.incrementAndGet();
                    w.start();
                    log("[Pool] Created new worker due to load: " + w.getName());
                    log("[Pool] Task accepted into new queue #" + (queues.size() - 1) + ": " + taskDescription(command));
                    taskAdded = true;
                } else {
                    log("[Rejected] Task " + taskDescription(command) + " was rejected due to overload!");
                    throw new RejectedExecutionException("Task rejected due to overload");
                }
            }

            int freeThreads = 0;
            for (BlockingQueue<Runnable> q : queues) {
                if (q.isEmpty()) freeThreads++;
            }
            if (freeThreads < minSpareThreads && currentPoolSize.get() < maxPoolSize) {
                BlockingQueue<Runnable> spareQueue = new ArrayBlockingQueue<>(queueSize);
                queues.add(spareQueue);
                Worker spareWorker = new Worker(spareQueue);
                workers.add(spareWorker);
                currentPoolSize.incrementAndGet();
                spareWorker.start();
                log("[Pool] Spare worker created due to low free threads: " + spareWorker.getName());
            }
        }
    }

    public <T> Future<T> submit(Callable<T> callable) {
        if (callable == null) throw new NullPointerException("Callable is null");
        FutureTask<T> futureTask = new FutureTask<>(callable);
        execute(futureTask);
        return futureTask;
    }

    public void shutdown() {
        synchronized (mainLock) {
            if (isShutdown) return;
            isShutdown = true;
            log("[Pool] Shutdown initiated.");
        }
        new Thread(() -> {
            synchronized (mainLock) {
                while (!workers.isEmpty()) {
                    try {
                        mainLock.wait(100);
                    } catch (InterruptedException ignored) {
                    }
                }
                isTerminated = true;
                log("[Pool] Shutdown complete.");
            }
        }, "Shutdown-Waiter").start();
    }

    public void shutdownNow() {
        synchronized (mainLock) {
            if (isShutdown) return;
            isShutdown = true;
            isTerminated = true;
            log("[Pool] Immediate shutdown initiated.");
            for (Worker w : new ArrayList<>(workers)) {
                w.shutdown();
            }
            workers.clear();
            queues.clear();
            currentPoolSize.set(0);
            log("[Pool] Immediate shutdown complete.");
        }
    }

    private static void log(String message) {
        System.out.printf("[%s] %s%n", java.time.LocalTime.now(), message);
    }

    public static void main(String[] args) throws InterruptedException {
        CustomThreadPool pool = new CustomThreadPool(
                2, // corePoolSize
                4, // maxPoolSize
                5, TimeUnit.SECONDS, // keepAliveTime
                5, // queueSize
                1  // minSpareThreads
        );

        for (int i = 1; i <= 10; i++) {
            final int taskId = i;
            try {
                pool.execute(() -> {
                    log("[Task] Started task #" + taskId);
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ignored) {
                    }
                    log("[Task] Finished task #" + taskId);
                });
            } catch (RejectedExecutionException e) {
                log("[Main] Task #" + taskId + " rejected.");
            }
        }

        Thread.sleep(15000);

        for (int i = 11; i <= 20; i++) {
            final int taskId = i;
            try {
                pool.execute(() -> {
                    log("[Task] Started task #" + taskId);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignored) {
                    }
                    log("[Task] Finished task #" + taskId);
                });
            } catch (RejectedExecutionException e) {
                log("[Main] Task #" + taskId + " rejected due to overload.");
            }
        }

        pool.shutdown();

        Thread.sleep(10000);
        log("[Main] Main thread finished.");
    }
}
