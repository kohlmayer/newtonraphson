package de.linearbits.newtonraphson;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import de.linearbits.newtonraphson.NewtonRaphson2D.Result;

public class ThreadPool {
    
    /**
     * Class wrapping the thread object.
     */
    static class PoolThread implements Runnable {
        
        private final Condition          condition;
        private volatile int             currentIdx;
        private boolean                  isClosed;
        private boolean                  isWorking;
        private final Callable<Result>[] jobs;
        private final ReentrantLock      lock;
        private final Thread             mainThread;
        private int                      maxIterations;
        private final AtomicInteger      nextJob;
        private final ThreadPool         poolHolder;
        private final ResultHolder[]     results;
                                         
        private final Thread             self;
        private final PoolThread[]       threads;
        private final AtomicInteger      totalIterations;
                                         
        /**
         * Creates a new thread.
         * @param jobs
         * @param results
         * @param nextJob
         * @param lock
         * @param condition
         * @param mainThread
         * @param poolHolder
         * @param threads
         * @param totalIterations
         */
        public PoolThread(final Callable<Result>[] jobs, final ResultHolder[] results, final AtomicInteger nextJob, final ReentrantLock lock, final Condition condition, final Thread mainThread, final ThreadPool poolHolder, final PoolThread[] threads, final AtomicInteger totalIterations) {
            this.isWorking = true;
            this.jobs = jobs;
            this.isClosed = false;
            this.results = results;
            this.nextJob = nextJob;
            this.mainThread = mainThread;
            this.poolHolder = poolHolder;
            this.threads = threads;
            this.currentIdx = 0;
            this.lock = lock;
            this.condition = condition;
            this.totalIterations = totalIterations;
            this.self = new Thread(this);
            this.self.setDaemon(true);
            this.self.start();
        }
        
        /**
         * Shutdown the thread.
         */
        public synchronized void close() {
            this.isClosed = true;
            this.self.interrupt();
        }
        
        /**
         * Gets the id of the currently processed job.
         * @return
         */
        public int getCurrentIdx() {
            return this.currentIdx;
        }
        
        /**
         * Interrupts this thread.
         */
        public void interrupt() {
            this.self.interrupt();
        }
        
        /**
         * Returns true if the thread is shutdown.
         * @return
         */
        public synchronized boolean isClosed() {
            return this.isClosed;
        }
        
        /**
         * Is the thread working?
         * @return
         */
        public synchronized boolean isWorking() {
            return this.isWorking;
        }
        
        @Override
        public void run() {
            while (!isClosed()) {
                // Wait
                this.lock.lock();
                try {
                    this.isWorking = false;
                    this.condition.await();
                } catch (InterruptedException e) {
                    // Do nothing
                } finally {
                    this.isWorking = true;
                    this.lock.unlock();
                }
                
                // Start working
                try {
                    while ((this.currentIdx = this.nextJob.getAndIncrement()) < this.jobs.length) {
                        final Callable<Result> job = this.jobs[this.currentIdx];
                        final Result result = job.call();
                        final ResultHolder rh = new ResultHolder(result);
                        this.results[this.currentIdx] = rh;
                        this.totalIterations.addAndGet(rh.getResult().getIterationsPerTry());
                        
                        // I've found a result. No other job has to be calculated.
                        if (rh.getResult().getSolution() != null) {
                            this.nextJob.set(this.jobs.length);
                            
                            // Interrupt main thread if it calculates a later start value
                            if (this.poolHolder.getCurrentIdx() > this.currentIdx) {
                                this.mainThread.interrupt();
                            }
                            
                            // Interrupt all working threads, if not me and only if they are processing a later job
                            for (int i = 0; i < this.threads.length; i++) {
                                if ((this.threads[i] != this) && this.threads[i].isWorking() && (this.threads[i].getCurrentIdx() > this.currentIdx)) {
                                    this.threads[i].interrupt();
                                }
                            }
                            
                            break;
                        }
                        // Total number of iterations reached
                        if (this.totalIterations.get() > this.maxIterations) {
                            this.nextJob.set(this.jobs.length);
                        }
                        
                    }
                } catch (Exception e) {
                    // Do nothing.
                }
            }
        }
        
        /**
         * Sets the allowed maximal number of iterations.
         * @param maxIterations
         */
        public synchronized void setMaxIterations(int maxIterations) {
            this.maxIterations = maxIterations;
        }
    }
    
    /**
     * Helper class holding the result.
     * TODO: Remove
     * @author kohlmayer
     *         
     */
    static class ResultHolder {
        private final Result result;
        
        public ResultHolder(final Result result) {
            this.result = result;
        }
        
        public Result getResult() {
            return this.result;
        }
    }
    
    private final Condition          condition;
    private int                      idx;
    private final Callable<Result>[] jobs;
    private final ReentrantLock      lock;
    private final Thread             mainThread;
    private volatile int             mainThreadIdx;
    private final AtomicInteger      nextJob;
    private final int                numJobs;
                                     
    private final ResultHolder[]     results;
    private final PoolThread[]       threads;
    private final AtomicInteger      totalIterations;
    private int                      totalTries;
                                     
    /**
     * Create a new thread pool. Main thread acts as worker.
     * @param numThreads
     * @param numJobs
     */
    @SuppressWarnings("unchecked")
    public ThreadPool(int numThreads, int numJobs) {
        this.numJobs = numJobs;
        this.jobs = new Callable[numJobs];
        this.results = new ResultHolder[numJobs];
        this.mainThread = Thread.currentThread();
        
        this.lock = new ReentrantLock();
        this.condition = this.lock.newCondition();
        this.nextJob = new AtomicInteger(0);
        this.totalIterations = new AtomicInteger(0);
        this.mainThreadIdx = 0;
        this.idx = 0;
        
        // Create threads. Main thread is also considered to be a thread.
        this.threads = new PoolThread[numThreads - 1];
        for (int i = 0; i < this.threads.length; i++) {
            this.threads[i] = new PoolThread(this.jobs, this.results, this.nextJob, this.lock, this.condition, this.mainThread, this, this.threads, this.totalIterations);
        }
        
    }
    
    /**
     * Returns the total number of iterations.
     * @return
     */
    public int getTotalIterations() {
        return this.totalIterations.get();
    }
    
    /**
     * Returns the number of tries.
     * @return
     */
    public int getTotalTries() {
        return this.totalTries;
    }
    
    /**
     * Start all threads and returns the first found result.
     * @return
     */
    public Result invokeFirstResult(int maxIterations) {
        this.nextJob.set(0);
        this.totalIterations.set(0);
        this.totalTries = 0;
        
        // Set max iterations
        for (int i = 0; i < this.threads.length; i++) {
            this.threads[i].setMaxIterations(maxIterations);
        }
        
        // Wake all threads
        this.lock.lock();
        try {
            this.condition.signalAll();
        } finally {
            this.lock.unlock();
        }
        
        // Start working
        try {
            while ((this.mainThreadIdx = this.nextJob.getAndIncrement()) < this.jobs.length) {
                final Callable<Result> job = this.jobs[this.mainThreadIdx];
                final Result result = job.call();
                final ResultHolder rh = new ResultHolder(result);
                this.results[this.mainThreadIdx] = rh;
                this.totalIterations.addAndGet(rh.getResult().getIterationsPerTry());
                
                // I've found a result. No other job has to be calculated.
                if ((rh.getResult() != null) && (rh.getResult().getSolution() != null)) {
                    this.nextJob.set(this.jobs.length);
                    
                    // Interrupt all working threads, if they are processing a later job
                    for (int i = 0; i < this.threads.length; i++) {
                        if (this.threads[i].isWorking() && (this.threads[i].getCurrentIdx() > this.mainThreadIdx)) {
                            this.threads[i].interrupt();
                        }
                    }
                }
                
                // Total number of iterations reached
                if (this.totalIterations.get() > maxIterations) {
                    this.nextJob.set(this.jobs.length);
                }
                
            }
        } catch (Exception e) {
            // Do nothing.
        }
        
        // Wait until all threads are done
        for (int i = 0; i < this.threads.length; i++) {
            while (this.threads[i].isWorking()) {
                // Spin
            }
        }
        
        // Clear potential interrupt flag of main thread
        Thread.interrupted();
        
        // Check if result is available
        Result result = null;
        for (int i = 0; i < this.results.length; i++) {
            if (this.results[i] != null) {
                this.totalTries++;
                if ((result == null) && (this.results[i].getResult().getSolution() != null)) { // result found.
                    result = this.results[i].getResult();
                }
            }
            // Clear jobs and results
            this.jobs[i] = null;
            this.results[i] = null;
        }
        
        // No result found
        if (result == null) {
            result = new Result(false, 0, 0);
            this.totalTries = this.results.length;
        }
        
        this.idx = 0;
        
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
    public void submit(Callable<Result> job) {
        if (this.idx > this.numJobs) {
            throw new IllegalArgumentException("You can not submit more jobs than specified.");
        }
        this.jobs[this.idx] = job;
        this.idx++;
    }
    
    /**
     * Returns the index of the currently processed job.
     * @return
     */
    private int getCurrentIdx() {
        return this.mainThreadIdx;
    }
    
}
