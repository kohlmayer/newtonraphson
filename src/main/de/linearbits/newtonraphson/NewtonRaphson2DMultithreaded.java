/*
 * Copyright 2015 Fabian Prasser
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.linearbits.newtonraphson;

import java.util.concurrent.Callable;

/**
 * The class implements a multithreadeed version of the Newton-Raphson algorithm
 * 
 * @author Florian Kohlmayer
 */
public class NewtonRaphson2DMultithreaded extends NewtonRaphson2D {
    
    /** SVUID */
    private static final long serialVersionUID = -7468092557502640336L;
    
    /**
     * Helper method to create a ThreadPool<Result>. The returned value can be provided to the constructor.
     * @param numThreads
     * @return
     */
    public static final ThreadPool<Result> createPool(final int numThreads) {
        // final ThreadPool<Result> executor = Executors.newFixedThreadPool(numThreads, new ThreadFactory() {
        // int count = 0;
        //
        // @Override
        // public Thread newThread(final Runnable r) {
        // final Thread thread = new Thread(r);
        // thread.setDaemon(true);
        // thread.setName("NewtonRaphson Solver " + this.count++);
        // return thread;
        // }
        // });
        ThreadPool<Result> executor = new ThreadPool<>(numThreads, 17);
        return executor;
    }
    
    /** The number of threads */
    private int                numThreads;
                               
    /** The executor */
    private ThreadPool<Result> executor;
                               
    /**
     * Creates a new instance
     * @param function
     */
    public NewtonRaphson2DMultithreaded(final ThreadPool<Result> executor, final int numThreads, final Function<Vector2D, Pair<Vector2D, SquareMatrix2D>> function) {
        super(function);
        init(executor, numThreads);
    }
    
    /**
     * Creates a new instance
     * @param function
     * @param constraints
     */
    public NewtonRaphson2DMultithreaded(final ThreadPool<Result> executor,
                                        final int numThreads,
                                        final Function<Vector2D, Pair<Vector2D, SquareMatrix2D>> function,
                                        final Constraint2D... constraints) {
        super(function, constraints);
        init(executor, numThreads);
    }
    
    /**
     * Creates a new instance
     * @param functions
     * @param derivatives
     */
    public NewtonRaphson2DMultithreaded(final ThreadPool<Result> executor,
                                        final int numThreads,
                                        final Function<Vector2D, Vector2D> functions,
                                        final Function<Vector2D, SquareMatrix2D> derivatives) {
        super(functions, derivatives);
        init(executor, numThreads);
    }
    
    /**
     * Creates a new instance
     * @param functions
     * @param derivatives
     * @param constraints
     */
    public NewtonRaphson2DMultithreaded(final ThreadPool<Result> executor,
                                        final int numThreads,
                                        final Function<Vector2D, Vector2D> functions,
                                        final Function<Vector2D, SquareMatrix2D> derivatives,
                                        final Constraint2D... constraints) {
        super(functions, derivatives, constraints);
        init(executor, numThreads);
    }
    
    /**
     * Creates a new instance
     * @param function1
     * @param function2
     */
    public NewtonRaphson2DMultithreaded(final ThreadPool<Result> executor,
                                        final int numThreads,
                                        final Function2D function1,
                                        final Function2D function2) {
        super(function1, function2);
        init(executor, numThreads);
    }
    
    /**
     * Creates a new instance
     * @param functions
     * @param derivatives
     * @param constraints
     */
    public NewtonRaphson2DMultithreaded(final ThreadPool<Result> executor,
                                        final int numThreads,
                                        final Function2D functions,
                                        final Function2D derivatives,
                                        final Constraint2D... constraints) {
        super(functions, derivatives, constraints);
        init(executor, numThreads);
    }
    
    /**
     * Creates a new instance
     * @param function1
     * @param function2
     * @param derivatives
     */
    public NewtonRaphson2DMultithreaded(final ThreadPool<Result> executor,
                                        final int numThreads,
                                        final Function2D function1,
                                        final Function2D function2,
                                        final Function<Vector2D, SquareMatrix2D> derivatives) {
        super(function1, function2, derivatives);
        init(executor, numThreads);
    }
    
    /**
     * Creates a new instance
     * @param function1
     * @param function2
     * @param derivatives
     * @param constraints
     */
    public NewtonRaphson2DMultithreaded(final ThreadPool<Result> executor,
                                        final int numThreads,
                                        final Function2D function1,
                                        final Function2D function2,
                                        final Function<Vector2D, SquareMatrix2D> derivatives,
                                        final Constraint2D... constraints) {
        super(function1, function2, derivatives, constraints);
        init(executor, numThreads);
    }
    
    /**
     * Creates a new instance
     * @param function1
     * @param function2
     * @param derivative11
     * @param derivative12
     * @param derivative21
     * @param derivative22
     */
    public NewtonRaphson2DMultithreaded(final ThreadPool<Result> executor,
                                        final int numThreads,
                                        final Function2D function1,
                                        final Function2D function2,
                                        final Function2D derivative11,
                                        final Function2D derivative12,
                                        final Function2D derivative21,
                                        final Function2D derivative22) {
        super(function1, function2, derivative11, derivative12, derivative21, derivative22);
        init(executor, numThreads);
    }
    
    /**
     * Creates a new instance
     * @param function1
     * @param function2
     * @param derivative11
     * @param derivative12
     * @param derivative21
     * @param derivative22
     * @param constraints
     */
    public NewtonRaphson2DMultithreaded(final ThreadPool<Result> executor,
                                        final int numThreads,
                                        final Function2D function1,
                                        final Function2D function2,
                                        final Function2D derivative11,
                                        final Function2D derivative12,
                                        final Function2D derivative21,
                                        final Function2D derivative22,
                                        final Constraint2D... constraints) {
        super(function1, function2, derivative11, derivative12, derivative21, derivative22, constraints);
        init(executor, numThreads);
    }
    
    @Override
    public Vector2D solve() {
        return this.solve(new Vector2D(1.0d, 1.0d));
    }
    
    @Override
    public Vector2D solve(final Vector2D start) {
        if (numThreads == 0) {
            return super.solve(start);
        } else {
            return _solveMultithreaded(start);
        }
    }
    
    private Vector2D _solveMultithreaded(final Vector2D start) {
        
        final long totalStart = System.currentTimeMillis();
        int totalIterations = 0;
        int totalTries = 0;
        Result result = null;
        
        // Further tries are forked
        
        // Are startvalues present
        
        // Add start value
        executor.submit(new Callable<Result>() {
            @Override
            public Result call() throws Exception {
                return _try(start, totalStart);
            }
        });
        
        // For all other start values add jobs
        for (int i = 0; i < this.preparedStartValues.length; i++) {
            final Vector2D startValue = new Vector2D(preparedStartValues[i][0], preparedStartValues[i][1]);
            executor.submit(new Callable<Result>() {
                @Override
                public Result call() throws Exception {
                    return _try(startValue, totalStart);
                }
            });
        }
        
        // Get result
        result = executor.invokeFirstResult();
        totalIterations += result.getIterationsPerTry();
        totalTries++;
        
        result.setTriesTotal(totalTries);
        result.setTimeTotal((int) (System.currentTimeMillis() - totalStart));
        result.setIterationsTotal(totalIterations);
        
        // No solution found
        if (result.getSolution() == null) {
            this.measures = new NewtonRaphsonMeasures(result.getIterationsTotal(),
                                                      result.getTriesTotal(),
                                                      result.getTimeTotal(),
                                                      0d);
            return new Vector2D(Double.NaN, Double.NaN);
        } else {
            this.measures = new NewtonRaphsonMeasures(result.getIterationsTotal(),
                                                      result.getTriesTotal(),
                                                      result.getTimeTotal(),
                                                      result.getQuality());
            return result.getSolution();
        }
        
    }
    
    /**
     * Provide the ThreadPool<Result>. Resource management (e.g. shutdown) has to be done externally.
     * @param executor
     * @param numThreads
     */
    private void init(final ThreadPool<Result> executor, final int numThreads) {
        this.numThreads = numThreads;
        this.executor = executor;
    }
    
}
