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
		switch (op) {
		case EQUALS:
			estimate = (double) this.histogram[targetBucket] / this.totalTuples; // h/w/ntup
			break;
		case GREATER_THAN:
			if (v > this.maxVal) {
				return 0;
			} else if (v < this.minVal) {
				return 1;
			}
			for (int i = targetBucket; i < this.histogram.length; i++) {
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
			double b_part1 = this.estimateSelectivity(Predicate.Op.EQUALS, v);
			double b_right1 = this.minVal + (targetBucket+1)* this.bucketWidth;
			double b_fraction1 = (double) Math.abs(v-b_right1)/ this.bucketWidth;
			double b_selectivity1 = b_part1 * b_fraction1;
			double b_other1 = this.estimateSelectivity(Predicate.Op.LESS_THAN, (int) Math.ceil(b_right1-this.bucketWidth));
			estimate = b_selectivity1 + b_other1;
			break;
		case GREATER_THAN_OR_EQ:

			if (v > this.maxVal) {
				return 0;
			} else if (v < this.minVal) {
				return 1;
			}
			double b_part = this.estimateSelectivity(Predicate.Op.EQUALS, v);
			double b_right = this.minVal + (targetBucket + 1)* this.bucketWidth;
			double b_fraction = (double) Math.abs(b_right - v)/ this.bucketWidth;
			double b_selectivity = b_part * b_fraction;
			double b_other = this.estimateSelectivity(Predicate.Op.GREATER_THAN, (int) Math.ceil(b_right));
			estimate = b_selectivity + b_other;
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
		return Arrays.toString(this.histogram);
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
