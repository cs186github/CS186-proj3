package simpledb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;
    private int numTuples = 0;
    private int ioCost = IOCOSTPERPAGE;
    private HeapFile targetTable;
    private TupleDesc tableTD;
    private HashMap<Integer,Integer[]> intStats;
    private HashMap<Integer, IntHistogram> intHists;
    private HashMap<Integer, StringHistogram> stringHists;
    
    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
    	
    	
    	//NOTE: important design decision is made in the way we implement the 
    	// iteration process. If we do tuple by tuple but compute all fields 
    	// at the same time we take O(n) time but O(n^2) space. If we iterate 
    	// over each field and then by each tuple, we take O(n^2) time and
    	// O(n) space. 
    	
    	this.ioCost = ioCostPerPage;
    	this.targetTable = (HeapFile) Database.getCatalog().getDbFile(tableid);
    	this.tableTD = this.targetTable.getTupleDesc();
    	this.intStats = new HashMap<Integer,Integer[]>();
    	this.intHists = new HashMap<Integer,IntHistogram>();
    	this.stringHists = new HashMap<Integer,StringHistogram>();
    	
    	Transaction tran = new Transaction();
    	tran.start();
    	DbFileIterator iter = this.targetTable.iterator(tran.getId());
    	
    	// first scan through table, compute min and max. Fill in stringHist
    	try {
			iter.open();
			while(iter.hasNext()){
				Tuple tup = iter.next();
				for(int i=0;i<this.tableTD.numFields();i++){
					Field f = tup.getField(i);
					Type fieldType = f.getType();
					switch(fieldType){
					case INT_TYPE:

						Integer[] minAndMax = new Integer[2];
						int fieldValue = ((IntField) f).getValue();
						if(!this.intStats.containsKey(i)){
							minAndMax[0] = fieldValue;
							minAndMax[1] = fieldValue;
						}else{
							Integer min = this.intStats.get(i)[0];
							Integer max = this.intStats.get(i)[1];
							
							if(fieldValue<min){
								min = fieldValue;
							}
							if(fieldValue>max){
								max = fieldValue;
							}
							minAndMax[0] = min;
							minAndMax[1] = max;
							
						}

						this.intStats.put(i, minAndMax);
						break;
					case STRING_TYPE:
						StringField s = (StringField) tup.getField(i);
						StringHistogram h;
						if(!this.stringHists.containsKey(f)){
							h = new StringHistogram(NUM_HIST_BINS); 
							
						}else{
							h = this.stringHists.get(i);
						}
						h.addValue( ( ((StringField) f).getValue()));
						this.stringHists.put(i, h);
						break;
					default:
						System.out.println("Unsupported Field Type");
						break;
					}
				}
				this.numTuples += 1;
			}
			iter.rewind();
			
			// second iteration to actually populate intHistogram
			
			while(iter.hasNext()){
				Tuple tup = iter.next();
				for(Entry<Integer, Integer[]> fieldStats:this.intStats.entrySet()){
					int min = fieldStats.getValue()[0];
					int max = fieldStats.getValue()[1];
					
					IntHistogram h; 
					if(!this.intHists.containsKey(fieldStats.getKey())){
						h = new IntHistogram(NUM_HIST_BINS,min,max);
					} else {
						h = this.intHists.get(fieldStats.getKey());
					}
					int v = ((IntField) tup.getField(fieldStats.getKey())).getValue();
					
					h.addValue(v);
					this.intHists.put(fieldStats.getKey(), h);
				}
			}

			iter.close();

	    	tran.commit();
		} catch (DbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			
			e.printStackTrace();
		}
    	
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return this.targetTable.numPages()*this.ioCost;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
    	//TODO: no particular reason for ceil or floor
        return (int) Math.ceil(this.numTuples*selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
    	Type fieldType = this.tableTD.getFieldType(field);
    	// find field
    	switch(fieldType){
    	case INT_TYPE:
    		IntHistogram h = this.intHists.get(field);
    		return h.estimateSelectivity(op, ((IntField) constant).getValue());
    	case STRING_TYPE:
    		StringHistogram s = this.stringHists.get(field);
    		return s.estimateSelectivity(op, ((StringField) constant).getValue());
    	default:
    		System.out.println("Unsupported Type");
            return 1.0;
    	}
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.numTuples;
    }

}
