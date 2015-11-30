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

/**
 * The class implements the Newton-Raphson algorithm
 * 
 * @author Fabian Prasser
 * @author Florian Kohlmayer
 */
public class NewtonRaphson2D extends NewtonRaphsonConfiguration<NewtonRaphson2D> {
    
   public class Result {
        private final boolean  terminate;
        private final Vector2D solution;
        private final double   quality;
        private final int      iterationsPerTry;
        private final int      timePerTry;
        private int            iterationsTotal;
        private int            timeTotal;
        private int            triesTotal;
                               
        public Result(final boolean terminate, final int iterations, final long startTime) {
            this(terminate, null, -1d, iterations, startTime);
        }
        
        public Result(final boolean terminate, final Vector2D solution, final double quality, final int iterations, final long startTime) {
            this.terminate = terminate;
            this.solution = solution;
            this.quality = quality;
            this.iterationsPerTry = iterations;
            this.timePerTry = (int) (System.currentTimeMillis() - startTime);
        }
        
        public int getIterationsTotal() {
            return iterationsTotal;
        }
        
        public void setIterationsTotal(int iterationsTotal) {
            this.iterationsTotal = iterationsTotal;
        }
        
        public int getTimeTotal() {
            return timeTotal;
        }
        
        public void setTimeTotal(int timeTotal) {
            this.timeTotal = timeTotal;
        }
        
        public int getTriesTotal() {
            return triesTotal;
        }
        
        public void setTriesTotal(int triesTotal) {
            this.triesTotal = triesTotal;
        }
        
        public boolean isTerminate() {
            return terminate;
        }
        
        public Vector2D getSolution() {
            return solution;
        }
        
        public double getQuality() {
            return quality;
        }
        
        public int getIterationsPerTry() {
            return iterationsPerTry;
        }
        
        public int getTimePerTry() {
            return timePerTry;
        }
        
    }
    
    /** SVUID */
    private static final long                                        serialVersionUID = -2439285310000826600L;
                                                                                      
    /** Constraints */
    private final Constraint2D[]                                     constraints;
                                                                     
    /** Derivative functions */
    private final Function<Vector2D, SquareMatrix2D>                 derivativeFunction;
                                                                     
    /** A function implementing the object function and the derivate functions */
    private final Function<Vector2D, Pair<Vector2D, SquareMatrix2D>> masterFunction;
                                                                     
    /** Measures of the last run */
    protected NewtonRaphsonMeasures                                  measures;
                                                                     
    /** First and second function */
    private final Function<Vector2D, Vector2D>                       objectFunction;
                                                                     
    /** First function */
    private final Function2D                                         objectFunction1;
                                                                     
    /** Second function */
    private final Function2D                                         objectfunction2;
                                                                     
    /**
     * Creates a new instance
     * @param function
     */
    public NewtonRaphson2D(final Function<Vector2D, Pair<Vector2D, SquareMatrix2D>> function) {
        this(function, (Constraint2D[]) null);
    }
    
    /**
     * Creates a new instance
     * @param function
     * @param constraints
     */
    public NewtonRaphson2D(final Function<Vector2D, Pair<Vector2D, SquareMatrix2D>> function,
                           final Constraint2D... constraints) {
        masterFunction = function;
        objectFunction = null;
        derivativeFunction = null;
        objectFunction1 = null;
        objectfunction2 = null;
        this.constraints = constraints;
    }
    
    /**
     * Creates a new instance
     * @param functions
     * @param derivatives
     */
    public NewtonRaphson2D(final Function<Vector2D, Vector2D> functions,
                           final Function<Vector2D, SquareMatrix2D> derivatives) {
        this(functions, derivatives, (Constraint2D[]) null);
    }
    
    /**
     * Creates a new instance
     * @param functions
     * @param derivatives
     * @param constraints
     */
    public NewtonRaphson2D(final Function<Vector2D, Vector2D> functions,
                           final Function<Vector2D, SquareMatrix2D> derivatives,
                           final Constraint2D... constraints) {
        objectFunction = functions;
        derivativeFunction = derivatives;
        objectFunction1 = null;
        objectfunction2 = null;
        masterFunction = null;
        this.constraints = constraints;
    }
    
    /**
     * Creates a new instance. This variant will automatically derive the given functions.
     * @param function1
     * @param function2
     */
    public NewtonRaphson2D(final Function2D function1,
                           final Function2D function2) {
        this(function1, function2, (Constraint2D[]) null);
    }
    
    /**
     * Creates a new instance. This variant will automatically derive the given functions.
     * @param function1
     * @param function2
     * @param constraints
     */
    public NewtonRaphson2D(final Function2D function1,
                           final Function2D function2,
                           final Constraint2D... constraints) {
        objectFunction = null;
        derivativeFunction = null;
        objectFunction1 = function1;
        objectfunction2 = function2;
        masterFunction = null;
        this.constraints = constraints;
    }
    
    /**
     * Creates a new instance
     * @param function1
     * @param function2
     * @param derivatives
     */
    public NewtonRaphson2D(final Function2D function1,
                           final Function2D function2,
                           final Function<Vector2D, SquareMatrix2D> derivatives) {
        this(function1, function2, derivatives, (Constraint2D[]) null);
    }
    
    /**
     * Creates a new instance
     * @param function1
     * @param function2
     * @param derivatives
     * @param constraints
     */
    public NewtonRaphson2D(final Function2D function1,
                           final Function2D function2,
                           final Function<Vector2D, SquareMatrix2D> derivatives,
                           final Constraint2D... constraints) {
        objectFunction = null;
        derivativeFunction = derivatives;
        objectFunction1 = function1;
        objectfunction2 = function2;
        masterFunction = null;
        this.constraints = constraints;
    }
    
    /**
     * Creates a new instance. This variant will automatically derive the given functions.
     * @param function1
     * @param function2
     * @param derivative11
     * @param derivative12
     * @param derivative21
     * @param derivative22
     */
    public NewtonRaphson2D(final Function2D function1,
                           final Function2D function2,
                           final Function2D derivative11,
                           final Function2D derivative12,
                           final Function2D derivative21,
                           final Function2D derivative22) {
        this(function1, function2, derivative11, derivative12, derivative21, derivative22, (Constraint2D[]) null);
    }
    
    /**
     * Creates a new instance. This variant will automatically derive the given functions.
     * @param function1
     * @param function2
     * @param derivative11
     * @param derivative12
     * @param derivative21
     * @param derivative22
     * @param constraints
     */
    public NewtonRaphson2D(final Function2D function1,
                           final Function2D function2,
                           final Function2D derivative11,
                           final Function2D derivative12,
                           final Function2D derivative21,
                           final Function2D derivative22,
                           final Constraint2D... constraints) {
        objectFunction = null;
        this.constraints = constraints;
        derivativeFunction = new Function<Vector2D, SquareMatrix2D>() {
            SquareMatrix2D result = new SquareMatrix2D();
            
            @Override
            public SquareMatrix2D evaluate(final Vector2D input) {
                result.x1 = derivative11.evaluate(input);
                result.x2 = derivative12.evaluate(input);
                result.y1 = derivative21.evaluate(input);
                result.y2 = derivative22.evaluate(input);
                return result;
            }
        };
        objectFunction1 = function1;
        objectfunction2 = function2;
        masterFunction = null;
    }
    
    /**
     * Configures this instance
     * @param config
     */
    public NewtonRaphson2D configure(final NewtonRaphsonConfiguration<?> config) {
        accuracy = config.accuracy;
        iterationsPerTry = config.iterationsPerTry;
        iterationsTotal = config.iterationsTotal;
        timePerTry = config.timePerTry;
        timeTotal = config.timeTotal;
        preparedStartValues = config.preparedStartValues;
        if (preparedStartValues != null) {
            iterationsTotal = (preparedStartValues.length + 1) * iterationsPerTry; // Includes given start value
        }
        return this;
    }
    
    /**
     * Returns measurements for the last run
     */
    public NewtonRaphsonMeasures getMeasures() {
        return measures;
    }
    
    /**
     * Returns the vector of solutions obtained by the Newton-Raphson algorithm.
     */
    public Vector2D solve() {
        return this.solve(new Vector2D(1.0d, 1.0d));
    }
    
    /**
     * Returns the vector of solutions obtained by the Newton-Raphson algorithm.
     * 
     * @param start
     */
    public Vector2D solve(final Vector2D start) {
        return _solve(start);
    }
    
    /**
     * Implementation of the Newton-Raphson algorithm
     * @param start
     * @param constraints
     * @return
     */
    private Vector2D _solve(final Vector2D start) {
        Result result = null;
        if (preparedStartValues != null) {
            result = _solveValues(start, preparedStartValues, iterationsTotal, 0, preparedStartValues.length, true);
        } else {
            result = _solveRandom(start, iterationsTotal, true);
        }
        
        // No solution found
        if (result.getSolution() == null) {
            measures = new NewtonRaphsonMeasures(result.getIterationsTotal(),
                                                 result.getTriesTotal(),
                                                 result.getTimeTotal(),
                                                 0d);
            // Nothing found
            return new Vector2D(Double.NaN, Double.NaN);
        } else {
            measures = new NewtonRaphsonMeasures(result.getIterationsTotal(),
                                                 result.getTriesTotal(),
                                                 result.getTimeTotal(),
                                                 result.getQuality());
            return result.getSolution();
        }
    }
    
    protected Result _solveRandom(final Vector2D start, final int maxIterations, boolean useStartValue) {
        // Init math stuff
        final Vector2D init = start.clone();
        Vector2D solution = null;
        // Measure
        final long totalStart = System.currentTimeMillis();
        int totalIterations = 0;
        int totalTries = 0;
        
        Result result = null;
        
        // Solve
        while (totalIterations <= maxIterations) {
            if (useStartValue) {
                solution = start;
                useStartValue = false;
            } else {
                solution = new Vector2D(((Math.random() * 2d) - 1d) * init.x,
                                        ((Math.random() * 2d) - 1d) * init.y);
            }
            
            totalTries++;
            result = _try(solution, totalStart);
            totalIterations += result.getIterationsPerTry();
            
            // Immediate termination or solution found
            if (result.isTerminate() || (result.getSolution() != null)) {
                break;
            }
        }
        
        result.setTriesTotal(totalTries);
        result.setTimeTotal((int) (System.currentTimeMillis() - totalStart));
        result.setIterationsTotal(totalIterations);
        return result;
    }
    
    protected Result _solveValues(final Vector2D start, final double[][] values, final int maxIterations, final int startIndex, final int stopIndex, boolean useStartValue) {
        // Init math stuff
        Vector2D solution = null;
        // Measure
        final long totalStart = System.currentTimeMillis();
        int totalIterations = 0;
        int totalTries = 0;
        int currentOffset = startIndex;
        
        Result result = null;
        
        // Solve
        while ((totalIterations <= maxIterations) && (currentOffset < values.length) && (currentOffset < stopIndex)) {
            if (useStartValue) {
                solution = start;
                useStartValue = false;
            } else {
                solution = new Vector2D(values[currentOffset][0], values[currentOffset++][1]);
            }
            
            totalTries++;
            result = _try(solution, totalStart);
            totalIterations += result.getIterationsPerTry();
            
            // Immediate termination or solution found
            if (result.isTerminate() || (result.getSolution() != null)) {
                break;
            }
        }
        
        result.setTriesTotal(totalTries);
        result.setTimeTotal((int) (System.currentTimeMillis() - totalStart));
        result.setIterationsTotal(totalIterations);
        return result;
    }
    
    /**
     * Returns the a result object containing the solution if found.
     * 
     * @param solution
     * @param totalStart
     * @return
     */
    protected Result _try(final Vector2D solution, final long totalStart) {
        
        Vector2D object = new Vector2D();
        SquareMatrix2D derivatives = new SquareMatrix2D();
        final Derivation2D derivation = derivativeFunction != null ? null : new Derivation2D();
        int iterations = 0;
        int totalIterations = 0;
        
        // Init timers
        final long startPerTry = System.currentTimeMillis();
        
        // Loop
        while (true) {
            
            // Check if thread has been interrupted
            if (Thread.interrupted()) {
                return new Result(true, totalIterations, startPerTry);
            }
            
            totalIterations++;
            
            // Without master function
            if (masterFunction == null) {
                
                // Evaluate object function
                if (objectFunction == null) {
                    object.x = objectFunction1.evaluate(solution);
                    object.y = objectfunction2.evaluate(solution);
                } else {
                    object = objectFunction.evaluate(solution);
                }
                
                // Break
                if ((Math.abs(object.x) <= accuracy) && (Math.abs(object.y) <= accuracy)) {
                    
                    // Calculate measures
                    final double quality = 1.0d - Math.sqrt((object.x * object.x) + (object.y * object.y));
                    
                    // Return
                    return new Result(false, solution, quality, totalIterations, startPerTry);
                }
                
                // Derive
                if (derivativeFunction == null) {
                    derivatives.x1 = derivation.evaluateDerivativeFunction1(objectFunction1, solution, object.x);
                    derivatives.x2 = derivation.evaluateDerivativeFunction2(objectFunction1, solution, object.x);
                    derivatives.y1 = derivation.evaluateDerivativeFunction1(objectfunction2, solution, object.y);
                    derivatives.y2 = derivation.evaluateDerivativeFunction2(objectfunction2, solution, object.y);
                } else {
                    derivatives = derivativeFunction.evaluate(solution);
                }
                
                // With master function
            } else {
                
                // Evaluate object function and derivatives
                final Pair<Vector2D, SquareMatrix2D> results = masterFunction.evaluate(solution);
                object = results.first;
                derivatives = results.second;
                
                // Break
                if ((Math.abs(object.x) <= accuracy) && (Math.abs(object.y) <= accuracy)) {
                    
                    // Calculate measures
                    final double quality = 1.0d - Math.sqrt((object.x * object.x) + (object.y * object.y));
                    
                    // Return
                    return new Result(false, solution, quality, totalIterations, startPerTry);
                }
            }
            
            // Compute difference
            derivatives.inverse();
            object.times(derivatives);
            solution.minus(object);
            
            
            // Error or constraint reached
            final long time = System.currentTimeMillis();
            if (solution.isNaN() ||
                (iterations++ >= iterationsPerTry) ||
                ((time - startPerTry) > timePerTry)) {
                return new Result(false, totalIterations, startPerTry);
            }
            
            // Check constraints
            if (constraints != null) {
                for (final Constraint2D constraint : constraints) {
                    if (!constraint.evaluate(solution)) {
                        return new Result(false, totalIterations, startPerTry);
                    }
                }
            }
            
            // Timing limit
            if ((time - totalStart) > timeTotal) {
                return new Result(true, totalIterations, startPerTry);
            }
        }
    }
}
