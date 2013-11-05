package simpledb;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

	// An that will store the histogram.
	private int[] histogram;
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
    	
    	this.histogram = new int[buckets]; // Java array initialized to all 0's by default
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
    	int targetBucket = this.mapValueToBucket(v);
        this.histogram[targetBucket] += 1;
    	
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
    	int targetBucket = this.mapValueToBucket(v);
    	double estimate = 0.0;
    	double bucketValue = 0;
    	double bucketSelectivity = 0;
    	double bucketFraction = 0;
		double bucketSum = 0;
    	switch(op){
    	case EQUALS:
    		estimate = (double) this.histogram[targetBucket]/this.totalTuples;
    		break;
    	case GREATER_THAN:
    		for(int i=targetBucket;i<this.histogram.length;i++){
    			bucketSum += this.histogram[i];
    		}
    		estimate = bucketSum/this.totalTuples;
    		break;
    	case LESS_THAN:
    		for(int i=0;i<targetBucket;i++){
    			bucketSum += this.histogram[i];
    		}
    		estimate = bucketSum/this.totalTuples;
    		break;
    	case LESS_THAN_OR_EQ:
    		if(v <= this.minVal){
    			return 0;
    		} else if(v >= this.maxVal){
    			return 1;
    		}
    		bucketValue = (targetBucket)*this.bucketWidth;
    		// We assume uniform distribution within the bucket
    		double fractionBucket = Math.abs(bucketValue-v)/this.bucketWidth;
    		bucketSelectivity = fractionBucket*this.histogram[targetBucket];
    		
    		estimate = bucketSelectivity;
    		for(int i=0;i<targetBucket;i++){
    			bucketSum += this.histogram[i];
    		}
    		estimate += bucketSum/this.totalTuples;
    		break;
    	case GREATER_THAN_OR_EQ:
    		if(v >= this.maxVal){
    			return 0;
    		} else if(v<=this.minVal){
    			return 1;
    		}
    		bucketValue = (targetBucket+1)*this.bucketWidth;
    		
    		// We assume uniform distribution within the bucket
    		fractionBucket = Math.abs(bucketValue-v)/this.bucketWidth;
    		bucketSelectivity = fractionBucket*this.histogram[targetBucket];
    		
    		estimate = bucketSelectivity;

    		for(int i=targetBucket+1;i<this.histogram.length;i++){
    			bucketSum += this.histogram[i];
    		}
    		estimate += bucketSum/this.totalTuples;
    		break;
    	case NOT_EQUALS:
    		estimate = (double) 1-this.histogram[targetBucket]/this.totalTuples;;
    		break;
    	case LIKE:
        	//TODO: According to GSI's response handle LIKE as EQUALS
    		estimate = (double) this.histogram[targetBucket]/this.totalTuples;
    		break;
		default:
    		estimate = (double) this.histogram[targetBucket]/this.totalTuples;
    		break;
    	}
    	System.out.println("estimate: "+estimate);
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
        return 1/this.histogram.length;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return Arrays.toString(this.histogram);
    }
    
    /**
     * Helper function for mapping value to bucket.
     */
    private int mapValueToBucket(int value){
    	if(value == this.maxVal){
    		return this.histogram.length-1;
    	}else if(value > this.maxVal){
    		return this.histogram.length;
    	}else if(value <= this.minVal){
    		return 0;
    	}else {
        	// subtract the minimum for correct bucket placement
        	int normalizedV = Math.abs(value - this.minVal);
        	return (int) Math.floor(normalizedV/this.bucketWidth);
    	}
    }
}
