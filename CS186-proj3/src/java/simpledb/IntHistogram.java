package simpledb;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

	// An that will store the histogram.
	private int[] histogramBuckets;
	private int maxVal;
	private int minVal;
	private double bucketWidth;
	private int totalTuples;
	
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	
    	this.histogramBuckets = new int[buckets]; // Java array initialized to all 0's by default
    	this.maxVal = max;
    	this.minVal = min;
    	this.bucketWidth = (double) (this.maxVal - this.minVal) / buckets;
    	this.totalTuples = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    	
    	this.totalTuples += 1;
    	
    	// subtract the minimum for correct bucket placement
    	// TODO: make sure handles case when minVal is negative
    	int normalizedV = Math.abs(v - this.minVal);

    	int targetBucket = (int) Math.floor(normalizedV / this.bucketWidth);
    	if(normalizedV == this.maxVal){
    		targetBucket = this.histogramBuckets.length -1;
    	}
    	
        this.histogramBuckets[targetBucket] += 1;
    	
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
    	int targetBucket = (int) ((v-this.minVal)/this.bucketWidth);
    	double estimate = -1.0;
    	switch(op){
    	case EQUALS:
    		estimate = (double) this.histogramBuckets[targetBucket]/this.totalTuples;
    		break;
    	case GREATER_THAN:
    		
    		break;
    	case LESS_THAN:
    		break;
    	case LESS_THAN_OR_EQ:
    		break;
    	case GREATER_THAN_OR_EQ:
    		break;
    	case NOT_EQUALS:
    		estimate = (double) (this.histogramBuckets.length - this.histogramBuckets[targetBucket])/this.totalTuples;
    		break;
    	case LIKE:
        	//TODO: Not sure how to handle LIKE operator
    		break;
		default:
			break;
    	}
        return estimate;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1/this.histogramBuckets.length;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return Arrays.toString(this.histogramBuckets);
    }
}
