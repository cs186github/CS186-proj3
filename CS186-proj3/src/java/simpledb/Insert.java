package simpledb;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator childIterator;
    private int tableId;
    
    private ArrayList<Tuple> iter; 
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid) throws DbException {
        // some code goes here

    	this.tid = t;
    	this.childIterator = child;
    	this.tableId = tableid;
    	if(!Database.getCatalog().getTupleDesc(tableid).equals(child.getTupleDesc())){
    		throw new DbException("Table TupleDesc does not match that of tuple");
    	}
    }

    public TupleDesc getTupleDesc() {
        // some code goes here

    	String[] fieldAr = {"NumInserted"};
    	Type[] typeAr = {Type.INT_TYPE};

    	TupleDesc td = new TupleDesc(typeAr, fieldAr);
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	this.iter = new ArrayList<Tuple>();
    	int numInserted = 0;
    	super.open();
    	this.childIterator.open();
    	while(this.childIterator.hasNext()){
    		Tuple nextTuple = this.childIterator.next();
    		try {
				Database.getBufferPool().insertTuple(this.tid,this.tableId,nextTuple);
	    		numInserted++;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	Tuple tup = new Tuple(this.getTupleDesc());
    	tup.setField(0, new IntField(numInserted));
        this.iter.add(tup);
    }

    public void close() {
        // some code goes here
    	this.childIterator.close();
    	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	this.open();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if(!this.iter.isEmpty()){
    		return this.iter.remove(0);
    	}
    	return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	DbIterator[] children = {this.childIterator};
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	this.childIterator = children[0];
    }
}
