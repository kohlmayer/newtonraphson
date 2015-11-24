package de.linearbits.newtonraphson;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadPool<T> {
    
    /**
     * Class wrapping the thread object.
     * @param <T>
     */
    static class PoolThread<T> extends Thread {
        
        private final BlockingQueue<Callable<T>> queue;
        private boolean                          isClosed;
        private final Object[]                   results;
        private final int                        numThread;
        private final Thread                     mainThread;
        private AtomicInteger                    latch;
                                                 
        public PoolThread(final int numThread, final Object[] results, BlockingQueue<Callable<T>> queue, Thread mainThread) {
            this.queue = queue;
            this.isClosed = false;
            this.numThread = numThread;
            this.results = results;
            this.mainThread = mainThread;
            this.setDaemon(true);
        }
        
        public synchronized void close() {
            this.isClosed = true;
            interrupt(); // break pool thread out of take() call.
        }
        
        public synchronized boolean isClosed() {
            return this.isClosed;
        }
        
        @Override
        public void run() {
            while (!isClosed()) {
                try {
                    
                    Callable<T> runnable = this.queue.take();
                    this.results[this.numThread] = null;
                    T result = runnable.call();
                    this.results[this.numThread] = result;
                    this.latch.decrementAndGet();
                    
                    // Clear potential interrupted state
                    Thread.interrupted();
                    
                    // Got result interrupt main thread
                    if (result != null) {
                        this.mainThread.interrupt();
                    }
                    
                } catch (Exception e) {
                    // Do nothing.
                }
            }
        }
        
        protected synchronized void setLatch(AtomicInteger latch) {
            this.latch = latch;
        }
    }
    
    private final BlockingQueue<Callable<T>> queue;
    private final int                        numThreads;
    private final PoolThread<T>[]            threads;
    private final Callable<T>[]              jobs;
    private volatile Object[]                results;
    private int                              count;
                                             
    /**
     * Create a new thread pool. Main thread acts as worker.
     * @param numThreads
     */
    @SuppressWarnings("unchecked")
    public ThreadPool(int numThreads) {
        this.numThreads = numThreads;
        this.jobs = new Callable[numThreads];
        this.results = new Object[numThreads];
        this.count = 0;
        this.queue = new LinkedBlockingQueue<>();
        
        // Create threads
        this.threads = new PoolThread[numThreads - 1];
        for (int i = 0; i < this.threads.length; i++) {
            this.threads[i] = new PoolThread<T>((i + 1), this.results, this.queue, Thread.currentThread());
            this.threads[i].start();
        }
        
    }
    
    @SuppressWarnings("unchecked")
    public T invokeFirstResult() {
        // Clear results
        for (int i = 0; i < this.results.length; i++) {
            this.results[i] = null;
        }
        
        AtomicInteger latch = new AtomicInteger(this.numThreads);
        for (int i = 0; i < this.threads.length; i++) {
            this.threads[i].setLatch(latch);
        }
        
        // Job with index 0 will be given to the main thread
        for (int i = 1; i < this.jobs.length; i++) {
            this.queue.offer(this.jobs[i]);
        }
        
        try {
            this.results[0] = this.jobs[0].call();
            latch.decrementAndGet();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        // Busy wait for first result
        T result = null;
        outer: while (true) {
            // If all threads finished only iterate once and exit
            if (latch.get() == 0) {
                for (int i = 0; i < this.results.length; i++) {
                    result = (T) this.results[i];
                    if (result != null) {
                        break outer;
                    }
                }
                break;
            } else {
                for (int i = 0; i < this.results.length; i++) {
                    result = (T) this.results[i];
                    if (result != null) {
                        break outer;
                    }
                }
            }
        }
        
        // Interrupt all threads
        for (int i = 0; i < this.threads.length; i++) {
            this.threads[i].interrupt();
        }
        
        // Busy wait until all threads finished
        while (true) {
            if (latch.get() == 0) {
                break;
            }
        }
        
        // Clear potential interrupted state
        Thread.interrupted();
        
        // Clear jobs
        for (int i = 0; i < this.jobs.length; i++) {
            this.jobs[i] = null;
        }
        
        this.count = 0;
        
        return result;
    }
    
    /**
     * Shutdown the thread pool.
     */
    public void shutdown() {
        for (int i = 0; i < this.threads.length; i++) {
            this.threads[i].close();
        }
    }
    
    /**
     * Submits a new job into the queue. Execution start with invoke methods. Callables have to return null if no solution found.
     * @param job
     */
    public void submit(Callable<T> job) {
        if (this.count > this.numThreads) {
            throw new IllegalArgumentException("You can not submit more jobs than threads.");
        }
        this.jobs[this.count++] = job;
    }
    
}
