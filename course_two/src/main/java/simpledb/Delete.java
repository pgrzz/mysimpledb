package simpledb;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId transactionId;
    private DbIterator child;
    private TupleDesc returnDesc;
    private boolean hasAccessed=false;
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
        this.transactionId=t;
        this.child=child;
        this.returnDesc=new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return returnDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        hasAccessed=false;
        super.open();
        child.open();
    }

    public void close() {
        // some code goes here
        hasAccessed=false;
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
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
        if(hasAccessed){
            return null;
        }
        hasAccessed=true;
        int count=0;
        while (child.hasNext()){
            Tuple tuple=child.next();
            Database.getBufferPool().deleteTuple(transactionId,tuple);
            count++;
        }
        Tuple tuple=new Tuple(getTupleDesc());
        tuple.setField(0,new IntField(count));
        return tuple;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.child=children[0];
    }

}
