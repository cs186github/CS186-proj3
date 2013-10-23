package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private RandomAccessFile raf;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        try {
            this.file = f;
            this.raf = new RandomAccessFile(f, "rw");   
            this.tupleDesc = td;
        } catch (FileNotFoundException e) {
        	e.printStackTrace();
        }
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            byte[] pageData = new byte[BufferPool.PAGE_SIZE];
            this.raf.seek(pid.pageNumber()*BufferPool.PAGE_SIZE);
            this.raf.read(pageData);
            HeapPageId hpid = new HeapPageId(pid.getTableId(), pid.pageNumber());
            HeapPage page = new HeapPage(hpid, pageData);
            return page;
        } catch (IOException e) {
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
        this.raf.seek(page.getId().pageNumber()*BufferPool.PAGE_SIZE);
        this.raf.write(page.getPageData());
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) this.file.length()/BufferPool.PAGE_SIZE;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        ArrayList<Page> modifiedPages = new ArrayList<Page>();
        for (int i = 0; i < numPages(); i++) {
            HeapPageId hpid = new HeapPageId(getId(), i);
            HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, hpid, null);
            if (p.getNumEmptySlots() != 0) {
                p.insertTuple(t);
                p.markDirty(true, tid);
                modifiedPages.add(p);
                return modifiedPages;
            }
        }
        byte[] newPage = new byte[BufferPool.PAGE_SIZE];
        HeapPageId hpid = new HeapPageId(getId(), numPages());
        this.raf.seek(numPages()*BufferPool.PAGE_SIZE);
        this.raf.write(newPage);
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, hpid, null);
        p.insertTuple(t);
        p.markDirty(true, tid);
        modifiedPages.add(p);
        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), null);
        p.deleteTuple(t);
        p.markDirty(true, tid);
        return p;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
    	return new HeapFileIterator(this, tid);
    }

    public static class HeapFileIterator implements DbFileIterator {
        private HeapFile hf;
        private TransactionId tid;
        private int pageNumber;
        private Iterator<Tuple> iterator;

        public HeapFileIterator(HeapFile h, TransactionId t) {
            this.hf = h;
            this.tid = t;
            this.pageNumber = 0;
        }

        /**
         * Opens the iterator
         * @throws DbException when there are problems opening/accessing the database.
         */
        public void open() throws DbException, TransactionAbortedException {
            HeapPageId hpid = new HeapPageId(this.hf.getId(), 0);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(this.tid, hpid, null);
            this.iterator = page.iterator();
        }

        /** @return true if there are more tuples available. */
        public boolean hasNext() throws DbException, TransactionAbortedException {

            if (this.iterator == null) {
                return false;
            }

            if (this.iterator.hasNext()) {
                return true;
            }
            
            // Need to check next page
            for (int i = this.pageNumber+1; i < this.hf.numPages(); i++) {
                HeapPageId hpid = new HeapPageId(this.hf.getId(), i);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(this.tid, hpid, null);
                Iterator<Tuple> nextIterator = page.iterator();
                if (nextIterator.hasNext()) {
                    return true;
                }
            }

            return false;
        }

        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator.
         * @throws NoSuchElementException if there are no more tuples
         */
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (this.iterator == null) {
                throw new NoSuchElementException("Iterator is null.");
            }

            if (this.iterator.hasNext()) {
                return this.iterator.next();
            }
            
            // Move to the next page
            for (int i = this.pageNumber + 1; i < this.hf.numPages(); i++) {
                HeapPageId hpid = new HeapPageId(this.hf.getId(), i);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(this.tid, hpid, null);
                Iterator<Tuple> nextIterator = page.iterator();
                if (nextIterator.hasNext()) {
                    this.iterator = page.iterator();
                    this.pageNumber = i;
                    return this.iterator.next();
                }
            }

            throw new NoSuchElementException("No more tuples.");
        }

        /**
         * Resets the iterator to the start.
         * @throws DbException When rewind is unsupported.
         */
        public void rewind() throws DbException, TransactionAbortedException {
            HeapPageId hpid = new HeapPageId(this.hf.getId(), 0);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(this.tid, hpid, null);
            this.iterator = page.iterator();
            this.pageNumber = 0;
        }

        /**
         * Closes the iterator.
         */
        public void close() {
            this.iterator = null;
        }
    }

}

