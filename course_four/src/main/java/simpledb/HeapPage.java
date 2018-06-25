package simpledb;

import java.io.*;
import java.util.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *要注意修改元组的recordId
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    private HeapPageId pid;
    private TupleDesc td;
    private byte header[];
    private Tuple tuples[];
    private int numSlots;
    private TransactionId dirtyTransactionId;
    private byte[] oldData;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.PAGE_SIZE*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#PAGE_SIZE
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples(); //插槽数=页面总大小/(每个元组所占大小+头部表示元组大小的占用位)
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();

        try{
            // allocate and read the actual records of this page    存放的元组数量
            tuples = new Tuple[numSlots];
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
        //插槽数=(页面总大小(byte)* 8)（表示成bit） /(每个元组大小（byte）* 8 (bit) +额外头部表示元组大小的占用位 1bit)
        也就表示了在当前这个页面我们能够放的该元组数量

     <------------------------------------------------------------------------>
            header
        1,1,1,1,1,,1,1,1,1,1,0,0,
            body
     tuple(Field[] fields)  tuple  tuple ....

    */
    private int getNumTuples() {        
        // some code goes here
        if(numSlots!=0){
            return numSlots;
        }
        return (int)Math.floor(BufferPool.PAGE_SIZE * 8) /(td.getSize()* 8 +1);
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * 这里为什么要除以8.0 返回值需要的是Byte
     */
    private int getHeaderSize() {        
        
        // some code goes here
            double double_value=getNumTuples()/8.0;
          return (int) Math.ceil(double_value); //Math.ceil 向上取整
                 
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            return new HeapPage(pid,oldData);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        oldData = getPageData().clone();
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
    // some code goes here
        return  pid;

    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {    //读一个元组的大小的对象
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);            //TupleDesc td 表示有哪些元组 和 RecordId 聚合在 Tuple中
        RecordId rid = new RecordId(pid, slotId);   //RecordId 表示在某一个page上的某一个槽的位置
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);          //field 则是 TupleDesc上的List<TDItem> items 的打交道的domain对象
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.PAGE_SIZE;
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i=0; i<header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot      Field[]
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.PAGE_SIZE - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.PAGE_SIZE;
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page;  the tuple should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        int tupleNum=t.getRecordId().tupleno();
        if(!t.getRecordId().getPageId().equals(this.pid) ||!isSlotUsed(tupleNum)){
            throw new DbException("this tuple is not on this page");
        }
        tuples[t.getRecordId().tupleno()]=null;
        markSlotUsed(tupleNum,false);
    }

    /**
     * 修改一个byte的指定位置的bit
     * @param target    待修改的byte
     * @param posInByte bit的位置在target的偏移量，从右往左且从0开始算，取值范围为0到7
     * @param value     为true修改该bit为1,为false时修改为0
     * @return 修改后的byte
     */
    private byte editBitInByte(byte target, int posInByte, boolean value) {
        if (posInByte < 0 || posInByte > 7) {
            throw new IllegalArgumentException();
        }
        byte b = (byte) (1 << posInByte);//将1这个bit移到指定位置，例如pos为3,value为true，将得到00001000
        //如果value为1,使用字节00001000以及"|"操作可以将指定位置改为1，其他位置不变
        //如果value为0,使用字节11110111以及"&"操作可以将指定位置改为0，其他位置不变
        return value ? (byte) (target | b) : (byte) (target & ~b);
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     *
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        if (!td.equals(t.getTupleDesc())) throw new DbException("tupleDesc is mismatch");
        //不使用getNumTuples() == 0来判断是否没有可用的slot，因为要找到可用的slot本身就要遍历一次tuples数组
        //if(getNumTuples() == 0) throw new DbException("the page is full (no empty slots)");
        for(int i=0;i<getNumTuples();i++) {
            if (!isSlotUsed(i)) {
                tuples[i] = t;
                //修改tuple的信息，表明它现在存储在这个page上
               t.setRecordId(new RecordId(pid, i));
                markSlotUsed(i,true);
                return;
            }
        }
        throw new DbException("the page is full (no empty slots)");
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
	// not necessary for lab1

        this.dirtyTransactionId=dirty?tid:null;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
	// Not necessary for lab1
       return dirtyTransactionId;
    }

    /**
     * Returns the number of empty slots on this page.
     * 判断有多少个 0  bit
     */
    public int getNumEmptySlots() {
        // some code goes here
        int emptySlots = 0;
        for (int i = 0; i < getNumTuples(); i++) {
            if (!isSlotUsed(i)) {
                emptySlots++;
            }
        }
        return emptySlots;
    }
    int bitCount( int n)
    {
         int c =0 ; // 计数器
        while (n >0)
        {
            if((n &1) ==1) // 当前位是1
                ++c ; // 计数器加1
            n >>=1 ; // 移位
        }
        return c ;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // some code goes here
      //首先看是第几个槽
        int byteNum = i / 8;//计算在第几个字节
        int byteSlot= i%8;  //计算在该字节的第几位,从右往左算（这是因为JVM用big-ending）


        return isOne(header[byteNum],byteSlot);
    }

    /**
     * @param target    要判断的bit所在的byte
     * @param posInByte 要判断的bit在byte的从右往左的偏移量，从0开始
     * @return target从右往左偏移量pos处的bit是否为1
     */
    private boolean isOne(byte target, int posInByte) {
        // 例如该byte是11111011,pos是2(也就是0那个bit的位置)
        // 那么只需先左移7-2=5位即可通过符号位来判断，注意要强转

        return (byte) (target << (7 - posInByte)) < 0;
    }
//    另一个方法：
//    private static boolean isOne(byte target, int posInByte) {
//        // 例如该byte是11111011,pos是2(也就是0那个bit的位置)
//        // 那么只需先右移2位即可通过是否整除2来判断
//        //
//        return (target >> posInByte) % 2 == 1;
//    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1
        int byteNum = i/8;//计算在第几个字节
        int posInByte = i % 8;//计算在该字节的第几位,从右往左算（这是因为JVM用big-ending）
        header[byteNum] = editBitInByte(header[byteNum], posInByte, value);
        int t=1;
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        return new TupleIterator();
    }

    class  TupleIterator implements Iterator<Tuple>{

        /**
         * 例子：header数组与遍历过程各个量的变化如下
         * headers: [00101000]
         * index:   [01234567]
         * pos:     [00011222]
         * pos在找到一个为1的bit之后才加一
         */
        int len=getNumTuples()-getNumEmptySlots();
        int index;     //tuple数组的下标变化
        int pos;
        @Override
        public boolean hasNext() {
        return index<getNumTuples() && pos<len;
        }

        @Override
        public Tuple next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            while (!isSlotUsed(index)){index++;}
            pos++;
            return tuples[index++];
        }

        @Override
        public void remove() {
            if(index!=0 || pos!=0){
                throw new UnsupportedOperationException();
            }
        }
    }

}

