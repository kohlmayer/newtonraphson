package de.linearbits.newtonraphson;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import de.linearbits.newtonraphson.NewtonRaphson2D.Result;

public class ThreadPool {
    
    /**
     * Helper class wrapping a callable and an assigned result bucket.
     * TODO: Remove.
     */
    static class JobHolder {
        
        private final Callable<Result> job;
        private final int              resultBucket;
                                       
        public JobHolder(Callable<Result> job, int resultBucket) {
            this.job = job;
            this.resultBucket = resultBucket;
        }
        
        public Callable<Result> getJob() {
            return this.job;
        }
        
        public int getResultBucket() {
            return this.resultBucket;
        }
        
    }
    
    /**
     * Class wrapping the thread object.
     */
    static class PoolThread implements Runnable {
        
        private final JobHolder[]    jobs;
        private boolean              isClosed;
        private boolean              isWorking;
        private final ResultHolder[] results;
        private final Thread         self;
        private final AtomicInteger  nextJob;
        private final Thread         mainThread;
        private final ThreadPool     poolHolder;
        private volatile int         currentIdx;
        private final PoolThread[]   threads;
                                     
        private final Condition      condition;
        private final ReentrantLock  lock;
                                     
        public PoolThread(final JobHolder[] jobs, final ResultHolder[] results, final AtomicInteger nextJob, final ReentrantLock lock, final Condition condition, final Thread mainThread, final ThreadPool poolHolder, final PoolThread[] threads) {
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
            this.self = new Thread(this);
            this.self.setDaemon(true);
            this.self.start();
        }
        
        public synchronized void close() {
            this.isClosed = true;
            this.self.interrupt();
        }
        
        public int getCurrentIdx() {
            return this.currentIdx;
        }
        
        public void interrupt() {
            this.self.interrupt();
        }
        
        public synchronized boolean isClosed() {
            return this.isClosed;
        }
        
        public synchronized boolean isWorking() {
            return this.isWorking;
        }
        
        @Override
        public void run() {
            while (!isClosed()) {
                try {
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
                    while ((this.currentIdx = this.nextJob.getAndIncrement()) < this.jobs.length) {
                        final JobHolder jobHolder = this.jobs[this.currentIdx];
                        final Callable<Result> job = jobHolder.getJob();
                        final Result result = job.call();
                        final ResultHolder rh = new ResultHolder(result);
                        this.results[this.currentIdx] = rh;
                        
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
                    }
                } catch (Exception e) {
                    // Do nothing.
                }
            }
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
    
    private final int            numJobs;
    private int                  idx;
    private final PoolThread[]   threads;
    private final Thread         mainThread;
    private final JobHolder[]    jobs;
    private final ResultHolder[] results;
    private final Condition      condition;
    private final ReentrantLock  lock;
                                 
    private final AtomicInteger  nextJob;
    private volatile int         mainThreadIdx;
                                 
    /**
     * Create a new thread pool. Main thread acts as worker.
     * @param numThreads
     */
    public ThreadPool(int numThreads, int numJobs) {
        this.numJobs = numJobs;
        this.jobs = new JobHolder[numJobs];
        this.results = new ResultHolder[numJobs];
        this.mainThread = Thread.currentThread();
        
        this.lock = new ReentrantLock();
        this.condition = this.lock.newCondition();
        this.nextJob = new AtomicInteger(0);
        this.mainThreadIdx = 0;
        this.idx = 0;
        
        // Create threads. Main thread is also considered to be a thread.
        this.threads = new PoolThread[numThreads - 1];
        for (int i = 0; i < this.threads.length; i++) {
            this.threads[i] = new PoolThread(this.jobs, this.results, this.nextJob, this.lock, this.condition, this.mainThread, this, this.threads);
        }
        
    }
    
    /**
     * Start all threads and returns the first found result.
     * @return
     */
    public Result invokeFirstResult() {
        this.nextJob.set(0);
        
        // Wake all threads
        this.lock.lock();
        try {
            this.condition.signalAll();
        } finally {
            this.lock.unlock();
        }
        
        // Start working
        while ((this.mainThreadIdx = this.nextJob.getAndIncrement()) < this.jobs.length) {
            
            // TODO: check if totalIterations are reached --> use atomicInteger
            
            final JobHolder jobHolder = this.jobs[this.mainThreadIdx];
            final Callable<Result> job = jobHolder.getJob();
            Result result = null;
            try {
                result = job.call();
            } catch (Exception e) {
                // Do nothing.
            }
            final ResultHolder rh = new ResultHolder(result);
            this.results[this.mainThreadIdx] = rh;
            
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
        Result result = (this.results[0] != null) ? this.results[0].getResult() : null;
        for (int i = 0; i < this.results.length; i++) {
            if ((this.results[i] != null) && (this.results[i].getResult().getSolution() != null)) { // result found.
                result = this.results[i].getResult();
                break;
            }
        }
        
        // Clear jobs and results
        for (int i = 0; i < this.jobs.length; i++) {
            this.jobs[i] = null;
            this.results[i] = null;
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
        this.jobs[this.idx] = new JobHolder(job, this.idx);
        this.idx++;
    }
    
    /**
     * Returns the index of the currently processed thread.
     * @return
     */
    private int getCurrentIdx() {
        return this.mainThreadIdx;
    }
    
}
