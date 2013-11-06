package simpledb;

import java.util.Arrays;

/**
 * A class to represent a fixed-width histogram over a single integer-based
 * field.
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
	 * This IntHistogram should maintain a histogram of integer values that it
	 * receives. It should split the histogram into "buckets" buckets.
	 * 
	 * The values that are being histogrammed will be provided one-at-a-time
	 * through the "addValue()" function.
	 * 
	 * Your implementation should use space and have execution time that are
	 * both constant with respect to the number of values being histogrammed.
	 * For example, you shouldn't simply store every value that you see in a
	 * sorted list.
	 * 
	 * @param buckets
	 *            The number of buckets to split the input value into.
	 * @param min
	 *            The minimum integer value that will ever be passed to this
	 *            class for histogramming
	 * @param max
	 *            The maximum integer value that will ever be passed to this
	 *            class for histogramming
	 */
	public IntHistogram(int buckets, int min, int max) {
		// some code goes here

		this.histogram = new int[buckets]; // Java array initialized to all 0's
											// by default
		this.maxVal = max;
		this.minVal = min;
		this.bucketWidth = (double) (this.maxVal - this.minVal + 1) / buckets;
		this.totalTuples = 0;
	}

	/**
	 * Add a value to the set of values that you are keeping a histogram of.
	 * 
	 * @param v
	 *            Value to add to the histogram
	 */
	public void addValue(int v) {
		// some code goes here

		this.totalTuples += 1;
		int targetBucket = this.mapValueToBucket(v);
		this.histogram[targetBucket] += 1;

	}

	/**
	 * Estimate the selectivity of a particular predicate and operand on this
	 * table.
	 * 
	 * For example, if "op" is "GREATER_THAN" and "v" is 5, return your estimate
	 * of the fraction of elements that are greater than 5.
	 * 
	 * @param op
	 *            Operator
	 * @param v
	 *            Value
	 * @return Predicted selectivity of this particular operator and value
	 */
	public double estimateSelectivity(Predicate.Op op, int v) {

		// some code goes here
		int targetBucket = this.mapValueToBucket(v);
		double estimate = 0.0;
		double bucketSum = 0;
		double borderVal =0;
		double bucketPart = 0;
		switch (op) {
		case EQUALS:

			int bucketHeight = this.histogram[targetBucket];
			estimate = (double) (bucketHeight/this.bucketWidth) / this.totalTuples; // h/w/ntup
			break;
		case GREATER_THAN:
			if (v > this.maxVal) {
				return 0;
			} else if (v < this.minVal) {
				return 1;
			}
			// targetBucket+1 because we want to skip to next bucket
			for (int i = targetBucket+1; i < this.histogram.length; i++) {
				bucketSum += this.histogram[i];
			}
			estimate = bucketSum / this.totalTuples;
			break;
		case LESS_THAN:
			if (v > this.maxVal) {
				return 1;
			} else if (v < this.minVal) {
				return 0;
			}
			for (int i = 0; i < targetBucket; i++) {
				bucketSum += this.histogram[i];
			}
			estimate = bucketSum / this.totalTuples;
			break;
		case LESS_THAN_OR_EQ:
			if (v > this.maxVal) {
				return 1;
			} else if (v < this.minVal) {
				return 0;
			}
			bucketPart = this.estimateSelectivity(Predicate.Op.EQUALS, v);
			borderVal = this.minVal + (targetBucket+1)* this.bucketWidth;
			estimate = bucketPart * (double) Math.abs(v-borderVal)/ this.bucketWidth;
			estimate += this.estimateSelectivity(Predicate.Op.LESS_THAN, v);
			break;
		case GREATER_THAN_OR_EQ:

			if (v > this.maxVal) {
				return 0;
			} else if (v < this.minVal) {
				return 1;
			}
			bucketPart = this.estimateSelectivity(Predicate.Op.EQUALS, v);
			borderVal = this.minVal + (targetBucket + 1)* this.bucketWidth;
			estimate = bucketPart * (double) Math.abs(borderVal - v)/ this.bucketWidth;
			estimate += this.estimateSelectivity(Predicate.Op.GREATER_THAN, v);
			break;
		case NOT_EQUALS:
			estimate = (double) 1 - this.estimateSelectivity(Predicate.Op.EQUALS, v);
			break;
		case LIKE:
			// According to GSI's response handle LIKE as EQUALS
			estimate = this.estimateSelectivity(Predicate.Op.EQUALS, v);
			break;
		default:
			estimate = this.estimateSelectivity(Predicate.Op.EQUALS, v);
			break;
		}
		
		return estimate;
	}

	/**
	 * @return the average selectivity of this histogram.
	 * 
	 *         This is not an indispensable method to implement the basic join
	 *         optimization. It may be needed if you want to implement a more
	 *         efficient optimization
	 * */
	public double avgSelectivity() {
		// some code goes here
		return 1 / this.histogram.length;
	}

	/**
	 * @return A string describing this histogram, for debugging purposes
	 */
	public String toString() {

		// some code goes here
		String outputString = new String("{");
		double count = this.minVal;
		for(int i = 0; i<this.histogram.length; i++){
			
			outputString += "["+count+"-"+(count+this.bucketWidth)+"]: "+this.histogram[i]+",";
			count += this.bucketWidth;
		}
		outputString += "}";
		return outputString;
	}

	/**
	 * Helper function for mapping value to bucket.
	 */
	private int mapValueToBucket(int value) {
		// subtract the minimum for correct bucket placement
		int normalizedV = Math.abs(value - this.minVal);
		return (int) Math.floor(normalizedV / this.bucketWidth);

	}
}
