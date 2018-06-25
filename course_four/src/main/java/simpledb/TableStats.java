package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
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

    static final int DEFAULT_PREHIST=100;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }


    private double totalIOCost;
    private  EstimatesPair[] pairs;
    private int totalTuple;

    /**
     *  用来帮助记录值
     */
    class EstimatesPair{
        int max;
        int min;
        int total;
        Type fieldType;
        String fieldName;


    }
    private Map<String,Object> pairs2Hist=new HashMap<>();
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (Exception e) {
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
           HeapFile dbFile=(HeapFile)Database.getCatalog().getDbFile(tableid);
         totalIOCost=dbFile.numPages()*ioCostPerPage;
         TupleDesc desc= dbFile.getTupleDesc();
        pairs=new EstimatesPair[dbFile.getTupleDesc().items.size()] ;
         for(int i=0;i<dbFile.getTupleDesc().items.size();i++){
             pairs[i]=new EstimatesPair();
             pairs[i].fieldType=dbFile.getTupleDesc().items.get(i).fieldType;
             pairs[i].fieldName=dbFile.getTupleDesc().items.get(i).toString();
         }
            try {
                DbFileIterator dbFileIterator=  dbFile.iterator(new TransactionId());
                 dbFileIterator.open();
              while (dbFileIterator.hasNext()){             // 第一次循环对于每一个值统计min,max,
                  Tuple tuple=dbFileIterator.next();

                  for(int i=0;i<desc.numFields();i++){
                      Field field= tuple.getField(i);
                      switch (field.getType()){
                          case INT_TYPE:
                              if(field instanceof IntField){
                                  int value=((IntField) field).getValue();
                                  if(pairs[i].max<value){
                                      pairs[i].max=value;
                                  }
                                  if(pairs[i].min>value){
                                      pairs[i].min=value;
                                  }
                                  pairs[i].total++;
                              }
                              break;
                          case STRING_TYPE:
                              pairs[i].total++;
                              break;
                    }

                  }
                totalTuple++;
              }

              dbFileIterator.rewind();

              for(int i=0;i<pairs.length;i++){          //对每一个值构建统计图
                    switch (pairs[i].fieldType){
                        case INT_TYPE:
                            pairs2Hist.put(pairs[i].fieldName,new IntHistogram(DEFAULT_PREHIST,pairs[i].min,pairs[i].max));
                            break;
                        case STRING_TYPE:
                            pairs2Hist.put(pairs[i].fieldName,new StringHistogram(DEFAULT_PREHIST));
                            break;
                    }
              }

              while (dbFileIterator.hasNext()){         //插入信息到统计中
                  Tuple tuple=dbFileIterator.next();
                  for(int i=0;i<desc.numFields();i++){
                      Field field=  tuple.getField(i);
                      switch (field.getType()){
                          case INT_TYPE:
                              if(field instanceof IntField){
                                  int value=((IntField) field).getValue();
                                  IntHistogram intHistogram= (IntHistogram) pairs2Hist.get(pairs[i].fieldName);
                                  intHistogram.addValue(value);
                              }
                              break;
                          case STRING_TYPE:
                              String value=field.toString();
                              StringHistogram stringHistogram= (StringHistogram) pairs2Hist.get(pairs[i].fieldName);
                              stringHistogram.addValue(value);
                              break;
                      }
                  }

              }
              dbFileIterator.close();
            }catch (Exception e){
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
        return totalIOCost;
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
        return  (int)Math.ceil(totalTuples()*selectivityFactor);
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
        EstimatesPair pair=pairs[field];
            switch (pair.fieldType){
                case STRING_TYPE:
                    StringHistogram stringHistogram= (StringHistogram) pairs2Hist.get(pair.fieldName);
                        return stringHistogram.avgSelectivity();
                case INT_TYPE:
                    IntHistogram intHistogram= (IntHistogram) pairs2Hist.get(pair.fieldName);
                        return intHistogram.avgSelectivity();
            }
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
        EstimatesPair pair=pairs[field];
        switch (pair.fieldType){
            case STRING_TYPE:
                StringHistogram stringHistogram= (StringHistogram) pairs2Hist.get(pair.fieldName);
                return stringHistogram.estimateSelectivity(op,constant.toString());
            case INT_TYPE:
                IntHistogram intHistogram= (IntHistogram) pairs2Hist.get(pair.fieldName);
                return intHistogram.estimateSelectivity(op,((IntField)constant).getValue());
        }
        return 1.0;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return totalTuple;
    }

}
