package simpledb;

import org.omg.CORBA.PRIVATE_MEMBER;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 * https://blog.csdn.net/tianlesoftware/article/details/5826546
 * 现在只有实现了BlockNestedLoopJoin    Hash的策略和 margeSort的策略详细见上面博客
 * 在多表联合查询的时候，如果我们查看它的执行计划，就会发现里面有多表之间的连接方式。
 * 多表之间的连接有三种方式：Nested Loops，Hash Join 和 Sort Merge Join.具体适用哪种类型的连接取决于
 当前的优化器模式 （ALL_ROWS 和 RULE）
 取决于表大小
 取决于连接列是否有索引
 取决于连接列是否排序
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate p;
    private DbIterator left;
    private DbIterator right;
    private  TupleDesc desc;
    private TupleIterator joinReuslt;

    // 131072是MySql中BlockNestedLoopJoin算法的默认缓冲区大小（以字节为单位）
    // 增大该参数可以更大程度减少磁盘IO，并充分利用已经优化过的内存中的Join算法
    // 对于测试案例中的两个大表的Join，使用默认大小需要25s，使用2倍大小需要15s，
    // 5倍需要10s，10倍则需要6s，所以权衡时间和空间的消耗来说，5倍比较合适
    private static final int blockMemory=131072*5;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        this.p=p;
        this.left=child1;
        this.right=child2;
        this.desc=  TupleDesc.merge(  child1.getTupleDesc(),   child2.getTupleDesc());
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return   desc.getFieldName(p.getField1());

    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return desc.getFieldName(p.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return desc ;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        left.open();
        right.open();
        if(p.getOperator().equals(Predicate.Op.EQUALS))
        {
            joinReuslt=HashJoin();
        }else{
            joinReuslt=NestedLoopJoin();
        }
        joinReuslt.open();
    }

    public void close() {
        // some code goes here
        super.close();
        left.close();
        right.close();
        joinReuslt.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        left.rewind();
        right.rewind();
        joinReuslt.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(joinReuslt.hasNext()){
            return joinReuslt.next();
        }
        return null;
    }



    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{this.left, this.right};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.left = children[0];
        this.right = children[1];
    }


    private TupleIterator NestedLoopJoin() throws DbException, TransactionAbortedException {
        List<Tuple> tuples=new ArrayList<>();
        Tuple  t1,t2;
        while (left.hasNext()){
            t1=left.next();
            right.rewind();
            while (right.hasNext()){
                t2=right.next();
                if(t1!=null && t2!=null && p.filter(t1,t2)){
                    tuples.add(mergeTuple(t1,t2));
                }
            }

        }
        return new TupleIterator(getTupleDesc(),tuples);
    }
    private TupleIterator BlockNestedLoopJoin() throws DbException, TransactionAbortedException {
        List<Tuple> tuples=new ArrayList<>();
        int blockSize = blockMemory / left.getTupleDesc().getSize();//131072是MySql中该算法的默认缓冲区大小 只用对左表缓存
        List<Tuple> leftCache=new ArrayList<>();

        while (left.hasNext()){
            leftCache.add(left.next());
            if(leftCache.size()>blockSize){
                right.rewind();
                while (right.hasNext()){
                     Tuple  t2=right.next();
                    leftCache.stream().filter(var->p.filter(var,t2)).forEach(var->tuples.add(mergeTuple(var,t2)));
                }
                leftCache.clear();
            }
        }
        // cache clear
        if(leftCache.size()>0){
            right.rewind();
            while (right.hasNext()){
                Tuple  t2=right.next();
                leftCache.stream().filter(var->p.filter(var,t2)).forEach(var->tuples.add(mergeTuple(var,t2)));
            }
            leftCache.clear();
        }

        return new TupleIterator(getTupleDesc(),tuples);
    }
    private TupleIterator HashJoin() throws DbException, TransactionAbortedException {
        List<Tuple> tuples=new ArrayList<>();
       //根据要join的Id进行分组
       HashMap<Integer,List<Tuple>> leftMap=new HashMap<>();    //一个桶对应分组JoinId相同的Tuple
       HashMap<Integer,List<Tuple>> rightMap=new HashMap<>();
       while (left.hasNext()){
           Tuple temp=left.next();
           int hashcode=temp.getField(p.getField1()).hashCode();
           List<Tuple> tempTuples= leftMap.get(hashcode);
           if(tempTuples==null ){
               tempTuples=new ArrayList<>();
           }
           tempTuples.add(temp);
           leftMap.put(hashcode,tempTuples);
       }
       while (right.hasNext()){
           Tuple temp=right.next();
           int hashcode=temp.getField(p.getField2()).hashCode();
           List<Tuple> tempTuples= rightMap.get(hashcode);
           if(tempTuples==null ){
               tempTuples=new ArrayList<>();
           }
           tempTuples.add(temp);
           rightMap.put(hashcode,tempTuples);
       }
       //THEN Nest LOOP JOIN
       leftMap.forEach((k,v)->{
           List<Tuple> rightTuples=rightMap.get(k);
            if(rightTuples!=null && rightTuples.size()>0){
                Iterator<Tuple> leftTupleIT=v.iterator();
                Tuple  t1,t2;
                while (leftTupleIT.hasNext()){
                    t1=leftTupleIT.next();
                    Iterator<Tuple> rightTupleIT=rightTuples.iterator();
                    while (rightTupleIT.hasNext()){
                        t2=rightTupleIT.next();
                        if(t1!=null && t2!=null && p.filter(t1,t2)){
                            tuples.add(mergeTuple(t1,t2));
                        }
                    }

                }
            }
       });

        return new TupleIterator(getTupleDesc(),tuples);
    }

    private Tuple mergeTuple(Tuple t1,Tuple t2){
        TupleDesc desc=TupleDesc.merge(t1.getTupleDesc(),t2.getTupleDesc());
        Tuple tuple=new Tuple(desc);
        int i=0;
        Iterator<Field> iter=t1.fields();
        while (iter.hasNext()){
            tuple.setField(i,iter.next());
            i++;
        }
        Iterator<Field> iter2=t2.fields();
        while (iter2.hasNext()){
            tuple.setField(i,iter2.next());
            i++;
        }
        return tuple;

    }

}
