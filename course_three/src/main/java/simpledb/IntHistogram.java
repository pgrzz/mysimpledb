package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 * todo 2018-5-29 该版本未实现 V(R,a) 对应的tuple统计关系再下一版本考虑
 */
public class IntHistogram {

    private int[] histogram;
    private int weight;
    private int nTuple;
    private int min;
    private int max;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        // weight=(max-min+1)/buckets;
        double range = (double) (max - min + 1) / buckets;
        weight = (int) Math.ceil(range);    //向上取正 默认 强制类型转换是向下取正
        histogram=new int[buckets];
        this.min=min;
        this.max=max;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int index=value2Index(v);
        histogram[index]=histogram[index]+1;
        nTuple++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // some code goes here
        int index=value2Index(v);
        int left = index * weight + min;
        int right=left+weight-1;
            double result=-1.0;
            double part_weight;
            double part_height;
            double part_tuple;
            int temp_tuple;
                switch (op){
                    case EQUALS:
                        if(v<min || v>max){
                            return 0.0;
                        }
                        result=(histogram[index])*1.0/weight/nTuple;    //能够这样子处理主要是因为有前提假设值满足均匀分布，但是值一般是服从zipfian分布
                        break;
                    case GREATER_THAN:
                        if(v<min){
                            return 1.0;
                        }else if(v>max){
                            return 0.0;
                        }
                         part_weight=(right-v)/weight;
                         part_height=(histogram[index]);
                         part_tuple=part_weight*part_height/nTuple;    //这里也是基于前提假设值满足 均匀分布
                        temp_tuple=0;
                        for(int i=index+1;i<histogram.length;i++){
                            temp_tuple+=histogram[i];
                        }
                        double pright=temp_tuple*1.0/nTuple;
                        result=part_tuple+pright;
                        break;
                    case LESS_THAN:
                        if(v<min){
                            return 0.0;
                        }else if(v>max){
                            return 1.0;
                        }
                         part_weight=(v-left)/weight;
                         part_height=(histogram[index]);
                         part_tuple=part_weight*part_height/nTuple;     ////这里也是基于前提假设值满足 均匀分布
                        temp_tuple=0;
                         for(int i=index-1;i>=0;i--){
                             temp_tuple+=histogram[i];
                         }
                        double pleft=temp_tuple*1.0/nTuple;
                        result=part_tuple+pleft;
                        break;
                    case NOT_EQUALS:
                        return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
                    case LIKE:
                        return avgSelectivity();
                    case LESS_THAN_OR_EQ:
                        return estimateSelectivity(Predicate.Op.LESS_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
                    case GREATER_THAN_OR_EQ:
                        return estimateSelectivity(Predicate.Op.GREATER_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
                }


        return result;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return null;
    }
    private int value2Index(int v){
        if(v==max){
            return histogram.length-1;
        }else{
            return (v-min)/weight;
        }
    }


}
