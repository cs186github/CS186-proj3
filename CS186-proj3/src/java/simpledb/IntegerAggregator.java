package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private HashMap<Field,Integer> countMap;
    private HashMap<Field,Integer> valueMap;
    
    private int groupByField;
    private Type groupByFieldType;
    private int aggregateField;
    private Op operator;
    
    private String aggColName;
    private String groupByColName;
    private TupleDesc aggTD;
    
    
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	
    	// TODO: test for the no grouping test!
    	this.groupByField = gbfield;
    	this.groupByFieldType = gbfieldtype;
    	this.aggregateField = afield;
    	this.operator = what;
    	
    	this.countMap = new HashMap<Field, Integer>();
    	this.valueMap = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field groupKey;
    	int currentValue = 0;
    	if(this.groupByField != Aggregator.NO_GROUPING){
    		if(this.aggColName == null){
        		this.aggColName = this.operator.name()+"("+tup.getTupleDesc().getFieldName(this.aggregateField)+")";
        	}
        	
        	if(this.groupByColName == null){
        		this.groupByColName = tup.getTupleDesc().getFieldName(this.groupByField);
        	}
        	groupKey = tup.getField(this.groupByField);
        	
    	} else {
    		groupKey = new IntField(-1);
    	}

    	int aggValue = ((IntField) tup.getField(this.aggregateField)).getValue();
    	
    	if(countMap.containsKey(groupKey)){
    		currentValue = countMap.get(groupKey);
    	}
    	countMap.put(groupKey, currentValue+1);
    	
    	switch(this.operator){
	    	case MIN:
	    		if(valueMap.containsKey(groupKey)){
	    			int groupValue = valueMap.get(groupKey);
	    			valueMap.put(groupKey, Math.min(groupValue, aggValue));
	    		}else{
	    			valueMap.put(groupKey, aggValue);
	    		}
	    		break;
	    	case MAX:
	    		if(valueMap.containsKey(groupKey)){
	    			int groupValue = valueMap.get(groupKey);
	    			valueMap.put(groupKey, Math.max(groupValue, aggValue));
	    		}else{
	    			valueMap.put(groupKey, aggValue);
	    		}
	    		break;
	    	case AVG:
	    		if(valueMap.containsKey(groupKey)){
	    			int groupValue = valueMap.get(groupKey);
	    			valueMap.put(groupKey, groupValue + aggValue);
	    		}else{
	    			valueMap.put(groupKey, aggValue);
	    		}
	    		break;
	    	case SUM:
	    		if(valueMap.containsKey(groupKey)){
	    			int groupValue = valueMap.get(groupKey);
	    			valueMap.put(groupKey, groupValue + aggValue);
	    		}else{
	    			valueMap.put(groupKey, aggValue);
	    		}
	    		break;
	    	case COUNT:
	    		if(valueMap.containsKey(groupKey)){
	    			int groupValue = valueMap.get(groupKey);
	    			valueMap.put(groupKey, groupValue+1);
	    		}else{
	    			valueMap.put(groupKey, 1);
	    		}
	    		break;
    		default:
    			// Do nothing?
    			break;
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	
    	Type[] tdTypes;
    	String[] tdNames;
    	
    	if(this.groupByField == Aggregator.NO_GROUPING){
    		tdTypes = new Type[1];
    		tdNames = new String[1];
    		
        	tdTypes[0] = Type.INT_TYPE;
        	tdNames[0] = aggColName;
    	} else {

    		tdTypes = new Type[2];
    		tdNames = new String[2];
    		
        	tdTypes[0] = this.groupByFieldType;
        	tdTypes[1] = Type.INT_TYPE;
        	tdNames[0] = this.groupByColName;
        	tdNames[1] = this.aggColName;
    	}
    	this.aggTD = new TupleDesc(tdTypes, tdNames);
    	
    	ArrayList<Tuple> tupArrayList = new ArrayList<Tuple>();
    	for(Entry<Field, Integer> entry: valueMap.entrySet()){
    		Tuple tup = new Tuple(this.aggTD);
    		if(this.groupByField == Aggregator.NO_GROUPING){
        		IntField fieldValue;
        		
        		if(this.operator == Aggregator.Op.AVG){
        			fieldValue = new IntField(entry.getValue()/countMap.get(entry.getKey()));
        		}else{
        			fieldValue = new IntField(entry.getValue());
        		}
        		tup.setField(0, fieldValue);
    		}else{
        		tup.setField(0, entry.getKey());
        		
        		IntField fieldValue;
        		
        		if(this.operator == Aggregator.Op.AVG){
        			fieldValue = new IntField(entry.getValue()/countMap.get(entry.getKey()));
        		}else{
        			fieldValue = new IntField(entry.getValue());
        		}
        		tup.setField(1, fieldValue);
    		}
    		tupArrayList.add(tup);
    	}
    	
    	TupleIterator t = new TupleIterator(this.aggTD, tupArrayList);
    	
    	return t;
    }

}
