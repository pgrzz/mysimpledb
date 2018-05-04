package simpledb;import java.io.*;
import java.nio.ByteBuffer;

import java.nio.channels.FileChannel;
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

    private File file;  //知道在哪个文件
    private TupleDesc tupleDesc;    //通过这个知道一个元组的大小和知道有多少元组
    private int numPage;

   private  Map<String,FileChannel> openFiles=new HashMap<>();

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file=f;
        this.tupleDesc=td;
        this.numPage=(int)(file.length()/BufferPool.PAGE_SIZE);
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
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
       return file.getAbsoluteFile().hashCode();    // tableId = heapFile.hashCode()
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
      return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        FileChannel channel=  openFiles.get(file.getAbsolutePath());
        openChannel(channel);
        channel=openFiles.get(file.getAbsolutePath());
        if(channel==null){
            throw new RuntimeException("chaneel打开失败");
        }
        int position=pid.pageNumber()*BufferPool.PAGE_SIZE; //起始位置偏移
        ByteBuffer dst=ByteBuffer.allocate(BufferPool.PAGE_SIZE);
        try {
            channel.read(dst,position);
            return new HeapPage((HeapPageId)pid,dst.array());
        } catch (IOException e) {
            e.printStackTrace();
        }


        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return numPage;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs

    /**
     *   //先 从 file 读每一个 page 再从每一个 page 读每一个元组
     */
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }
    private void openChannel(FileChannel channel){
        if(channel==null){  // doesn't open
            try {
                RandomAccessFile f=new RandomAccessFile(file,"rws");
                channel=f.getChannel();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            openFiles.put(file.getAbsolutePath(),channel);
        }
    }



    /**
     * 这个类在实现时有不少疑惑，参考了别人的代码才清楚以下一些点：
     * 1.tableid就是heapfile的id，即通过getId。。但是这个不是从0开始的，按照课程源码推荐，这是文件的哈希码。。
     * 2.PageId是从0开始的。。。(哪里说了，这个可以默认的么，谁知道这个作业的设计是不是从0开始。。。)
     * 3.transactionId哪里来的让我非常困惑，现在知道不用理，反正iterator方法的调用者会提供，应该是以后章节的内容
     * 4.我觉得别人的一个想法挺好，就是存储一个当前正在遍历的页的tuples迭代器的引用，这样一页一页来遍历
     */
    class HeapFileIterator implements DbFileIterator{

        int pageSize;
        int pageIndex;    // pageid 从0开始
        Iterator<Tuple> tupleIterator;
        TransactionId tid;

         HeapFileIterator(TransactionId tid) {
            this.pageSize = numPage;
            this.tid=tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pageIndex=0;
            HeapPageId pid = new HeapPageId(getId(), pageIndex);
            HeapPage page=(HeapPage)  Database.getBufferPool().getPage(tid,pid,Permissions.READ_ONLY);
            tupleIterator=page.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(tupleIterator==null){
                return false;
            }
            if(tupleIterator.hasNext()){
                    return true;
                //如果遍历完当前页，测试是否还有页未遍历
                //注意要减一，这里与for循环的一般判断逻辑（迭代变量<长度）不同，是因为我们要在接下来代码中将pagePos加1才使用
                //如果不理解，可以自己举一个例子想象运行过程  0-28
            }else if(pageIndex<pageSize-1){
                pageIndex++;            //  0-29 +1使用就是这个 虽然判断的 是28<30-1 但是还是使用的时候是 0-29页
                HeapPageId pid = new HeapPageId(getId(), pageIndex);
                HeapPage page=(HeapPage)  Database.getBufferPool().getPage(tid,pid,Permissions.READ_ONLY);
                tupleIterator=page.iterator();
                return hasNext();
            }else
                return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(!hasNext()){throw  new NoSuchElementException();}
            return tupleIterator.next();

        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
           open();
        }

        @Override
        public void close() {
            pageIndex=0;
            tupleIterator=null;
            FileChannel channel= openFiles.get(file.getAbsolutePath());
            if(channel!=null){
                try {
                    channel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            openFiles.remove(file.getAbsolutePath());
        }
    }

}

