package de.linearbits.newtonraphson;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

public class ThreadPool<T> {
    
    /**
     * Class wrapping a callable and an assigned result bucket.
     * @param <T>
     */
    static class JobHolder<T> {
        
        private final Callable<T> job;
        private final int         resultBucket;
                                  
        public JobHolder(Callable<T> job, int resultBucket) {
            this.job = job;
            this.resultBucket = resultBucket;
        }
        
        public Callable<T> getJob() {
            return this.job;
        }
        
        public int getResultBucket() {
            return this.resultBucket;
        }
        
    }
    
    /**
     * Class wrapping the thread object.
     * @param <T>
     */
    static class PoolThread<T> implements Runnable {
        
        private final BlockingQueue<JobHolder<T>> queue;
        private boolean                           isClosed;
        private final Object[]                    results;
        private final Thread                      self;
                                                  
        public PoolThread(final Object[] results, final BlockingQueue<JobHolder<T>> queue) {
            this.queue = queue;
            this.isClosed = false;
            this.results = results;
            this.self = new Thread(this);
            this.self.setDaemon(true);
            this.self.start();
        }
        
        public synchronized void close() {
            this.isClosed = true;
            this.self.interrupt(); // break pool thread out of take() call.
        }
        
        public void interrupt() {
            this.self.interrupt();
        }
        
        public synchronized boolean isClosed() {
            return this.isClosed;
        }
        
        @Override
        public void run() {
            while (!isClosed()) {
                try {
                    final JobHolder<T> runnable = this.queue.take();
                    final Callable<T> job = runnable.getJob();
                    final int resultBucket = runnable.getResultBucket();
                    this.results[resultBucket] = null;
                    final T result = job.call();
                    ResultHolder<T> rh = new ResultHolder<>(result);
                    this.results[resultBucket] = rh;
                } catch (Exception e) {
                    // Do nothing.
                }
            }
        }
    }
    
    static class ResultHolder<T> {
        private final T result;
        
        public ResultHolder(final T result) {
            this.result = result;
        }
        
        public T getResult() {
            return this.result;
        }
    }
    
    private final BlockingQueue<JobHolder<T>> queue;
    private final int                         numThreads;
    private final PoolThread<T>[]             threads;
    private final JobHolder<T>[]              jobs;
    private volatile ResultHolder<T>[]        results;
    private int                               count;
                                              
    /**
     * Create a new thread pool. Main thread acts as worker.
     * @param numThreads
     */
    @SuppressWarnings("unchecked")
    public ThreadPool(int numThreads) {
        this.numThreads = numThreads;
        this.jobs = new JobHolder[numThreads];
        this.results = new ResultHolder[numThreads];
        // Fill initially
        for (int i = 0; i < results.length; i++) {
            results[i] = new ResultHolder<>(null);
        }
        
        this.count = 0;
        this.queue = new LinkedBlockingQueue<>();
        
        // Create threads
        this.threads = new PoolThread[numThreads - 1];
        for (int i = 0; i < this.threads.length; i++) {
            this.threads[i] = new PoolThread<T>(this.results, this.queue);
        }
        
    }
    
    public T invokeFirstResult() {
        
        // Wait for all threads to finish and clear results
        int idx = 0;
        while (idx != this.numThreads) {
            if (this.results[idx] != null) {
                this.results[idx] = null;
                idx++;
            }
        }
        
        // Job with index 0 will be given to the main thread
        for (int i = 1; i < this.jobs.length; i++) {
            this.queue.add(this.jobs[i]);
        }
        
        try {
            this.results[this.jobs[0].getResultBucket()] = new ResultHolder<T>(this.jobs[0].getJob().call());
        } catch (Exception e) {
            // Do nothing.
        }
        
        // Busy wait for first result
        T result = null;
        idx = 0;
        while (idx != this.numThreads) {
            if (this.results[idx] != null) {
                if (this.results[idx].getResult() != null) {
                    result = this.results[idx].getResult();
                    break;
                }
                idx++;
            }
        }
        
        // Interrupt all threads
        for (int i = 0; i < this.threads.length; i++) {
            this.threads[i].interrupt();
        }
        
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
        this.jobs[this.count] = new JobHolder<>(job, this.count++);
    }
    
}
