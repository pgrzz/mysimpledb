package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupByField;
    private int aggregateByField;
    private Type groupByFieldType;//这个东西在构造返回的 tupleDesc时候有用
    private Op op;


    private Map<Field,List<Tuple>> groupTuple=new HashMap<>();
    private Map<Field,Integer>  groupValue=new HashMap<>();

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
        this.groupByField=gbfield;
        this.groupByFieldType=gbfieldtype;
        this.aggregateByField=afield;
        this.op=what;
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
        Field gField=null;
        if(groupByField!=-1){
            gField= tup.getField(groupByField);
        }


        List<Tuple> tuples=groupTuple.get(gField);
            if(tuples==null) {
                tuples = new ArrayList<>();
            }
           tuples.add(tup);
        groupTuple.put(gField,tuples);
        if (tup.getField(aggregateByField).getType() != Type.INT_TYPE) {
            throw new IllegalArgumentException("该tuple的指定列不是Type.INT_TYPE类型");
        }
        int tupValue;
        switch (op){
            case COUNT:
                groupValue.put(gField,groupTuple.get(gField).size());
                break;
            case SUM:

                    int sumValue=groupValue.get(gField)==null?0:groupValue.get(gField);
                sumValue+=((IntField)tup.getField(aggregateByField)).getValue();
                    groupValue.put(gField,sumValue);
                break;
            case MIN:

                    int min=groupValue.get(gField)==null?Integer.MAX_VALUE:groupValue.get(gField);
                     tupValue=((IntField)tup.getField(aggregateByField)).getValue();
                    min=min>tupValue?tupValue:min;
                    groupValue.put(gField,min);
                break;
            case MAX:

                    int max=groupValue.get(gField)==null?Integer.MIN_VALUE:groupValue.get(gField);
                    tupValue=((IntField)tup.getField(aggregateByField)).getValue();
                    max=max>tupValue?max:tupValue;
                    groupValue.put(gField,max);
                break;
            case AVG:

                    int result=tuples.stream().map(var->((IntField)var.getField(aggregateByField)).getValue()).reduce(0,(sum,item)->sum+=item);
                    groupValue.put(gField,result/tuples.size()) ;
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
        List<Tuple> result=new ArrayList<>();
        groupValue.forEach((k,v)->{
            Tuple tuple;
            if(groupByField==Aggregator.NO_GROUPING){
                List<Type> temp=new ArrayList<>();
                temp.add(groupByFieldType);
                tuple=new Tuple(new TupleDesc(temp.toArray(new Type[0])));
                tuple.setField(0,new IntField(v));
            }else{
                List<Type> temp=new ArrayList<>();
                temp.add(groupByFieldType);
                temp.add(Type.INT_TYPE);
                tuple=new Tuple(new TupleDesc(temp.toArray(new Type[0])));
                tuple.setField(0,k);
                tuple.setField(1,new IntField(v));
            }
            result.add(tuple);

        });
        if(result.size()==0){
            return new TupleIterator(new TupleDesc(new Type[]{groupByFieldType}),result);
        }
        return new TupleIterator(result.get(0).getTupleDesc(),result);

    }

}
