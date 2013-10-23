package simpledb;

import java.io.IOException;
import java.util.ArrayList;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator childIterator;
    
    private ArrayList<Tuple> iter;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	this.tid = t;
    	this.childIterator = child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	String[] fieldAr = {"NumDeleted"};
    	Type[] typeAr = {Type.INT_TYPE};
    	
    	TupleDesc td = new TupleDesc(typeAr, fieldAr);
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	this.iter = new ArrayList<Tuple>();
    	int numDeleted = 0;
    	super.open();
    	this.childIterator.open();
    	while(this.childIterator.hasNext()){
    		Tuple nextTuple = this.childIterator.next();
			Database.getBufferPool().deleteTuple(this.tid,nextTuple);
	    	numDeleted++;
    	}
    	Tuple tup = new Tuple(this.getTupleDesc());
    	tup.setField(0, new IntField(numDeleted));
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
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
