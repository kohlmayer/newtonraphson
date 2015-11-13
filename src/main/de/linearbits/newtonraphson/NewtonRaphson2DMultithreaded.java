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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

/**
 * The class implements the Newton-Raphson algorithm
 * 
 * @author Fabian Prasser
 */
public class NewtonRaphson2DMultithreaded extends NewtonRaphson2D {
    
    /** SVUID */
    private static final long serialVersionUID = -7468092557502640336L;
    
    /**
     * Helper method to create a ExecutorService. The returned value can be provided to the init method.
     * @param numThreads
     * @return
     */
    public static final ExecutorService createPool(final int numThreads) {
        final ExecutorService executor = Executors.newFixedThreadPool(numThreads, new ThreadFactory() {
            int count = 0;
            
            @Override
            public Thread newThread(final Runnable r) {
                final Thread thread = new Thread(r);
                thread.setDaemon(true);
                thread.setName("NewtonRaphson Solver " + count++);
                return thread;
            }
        });
        return executor;
    }
    
    /** The executor completion service */
    private CompletionService<Vector2D> executorCompletionService;
                                        
    /** The list of futures */
    private List<Future<Vector2D>>      futures;
                                        
    /** The number of threads */
    private int                         numThreads;
                                        
    /**
     * Creates a new instance
     * @param function
     */
    public NewtonRaphson2DMultithreaded(final ExecutorService executor, final int numThreads, final Function<Vector2D, Pair<Vector2D, SquareMatrix2D>> function) {
        super(function);
        init(executor, numThreads);
    }
    
    /**
     * Creates a new instance
     * @param function
     * @param constraints
     */
    public NewtonRaphson2DMultithreaded(final ExecutorService executor,
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
    public NewtonRaphson2DMultithreaded(final ExecutorService executor,
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
    public NewtonRaphson2DMultithreaded(final ExecutorService executor,
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
    public NewtonRaphson2DMultithreaded(final ExecutorService executor,
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
    public NewtonRaphson2DMultithreaded(final ExecutorService executor,
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
    public NewtonRaphson2DMultithreaded(final ExecutorService executor,
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
    public NewtonRaphson2DMultithreaded(final ExecutorService executor,
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
    public NewtonRaphson2DMultithreaded(final ExecutorService executor,
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
    public NewtonRaphson2DMultithreaded(final ExecutorService executor,
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
        if (executorCompletionService == null) {
            throw new IllegalArgumentException("No ExecutorService configured. Use 'init(ExecutorService executor, int numThreads)' before solving.");
        }
        return _solveMultithreaded(start);
    }
    
    private Vector2D _solveMultithreaded(final Vector2D start) {
        
        final int iterationsPerThread = (((double) iterationsTotal / (double) numThreads) <= 0) ? 1 : (int) ((double) iterationsTotal / (double) numThreads);
        
        final long totalStart = System.currentTimeMillis();
        
        // TODO: measures are not multihreadsafe!
        
        // Try start value in main thread
        final WorkerResult startResult = _try(start, totalStart);
        // Hard break
        if (startResult == null) {
            measures = new NewtonRaphsonMeasures(iterationsTotal, 0, (int) (System.currentTimeMillis() - totalStart), 0d);
            return new Vector2D(Double.NaN, Double.NaN);
        } else if ((startResult.getSolution() != null) && !startResult.getSolution().isNaN()) {
            measures = new NewtonRaphsonMeasures(iterationsTotal, 0, (int) (System.currentTimeMillis() - totalStart), 0d);
            return startResult.getSolution();
        }
        
        try {
            
            // Are startvalues present
            if (preparedStartValues != null) {
                
                int stepping = (int) ((double) preparedStartValues.length / (double) numThreads);
                if (stepping <= 0) {
                    stepping = 1;
                }
                
                // For each thread
                for (int i = 0; i < numThreads; i++) {
                    
                    // Execute
                    final int thread = i;
                    final int startIndex = thread * stepping;
                    final int stopIndex = thread == (numThreads - 1) ? preparedStartValues.length : (thread + 1) * stepping;
                    
                    // Worker thread
                    futures.add(executorCompletionService.submit(new Callable<Vector2D>() {
                        @Override
                        public Vector2D call() throws Exception {
                            return _solveValues(start, preparedStartValues, iterationsPerThread, startIndex, stopIndex, false);
                        }
                    }));
                }
                
            } else {
                // Use random guesses
                
                // For each thread
                for (int i = 0; i < numThreads; i++) {
                    // Execute
                    // Worker thread
                    futures.add(executorCompletionService.submit(new Callable<Vector2D>() {
                        @Override
                        public Vector2D call() throws Exception {
                            return _solveRandom(start, iterationsPerThread, false);
                        }
                    }));
                }
            }
            
            for (int i = 0; i < futures.size(); i++) {
                final Vector2D result = executorCompletionService.take().get();
                // Vector2D result = futures.get(i).get();
                if ((result != null) && !result.isNaN()) {
                    return result;
                }
            }
        } catch (
                InterruptedException
                | ExecutionException e) {
            e.printStackTrace();
        } finally {
            // Cancel all running threads
            for (final Future<Vector2D> f : futures) {
                f.cancel(true);
            }
            futures.clear();
        }
        
        // TODO: use the correct measures
        // Store measures
        measures = new NewtonRaphsonMeasures(iterationsTotal, 0, (int) (System.currentTimeMillis() - totalStart), 0d);
        
        // Nothing found
        return new Vector2D(Double.NaN, Double.NaN);
        
    }
    
    /**
     * Provide the ExecutorService. Resource management (e.g. shutdown) has to be done externally.
     * @param executor
     * @param numThreads
     */
    private void init(final ExecutorService executor, final int numThreads) {
        this.numThreads = numThreads;
        executorCompletionService = new ExecutorCompletionService<Vector2D>(executor);
        futures = new ArrayList<Future<Vector2D>>();
    }
    
}
