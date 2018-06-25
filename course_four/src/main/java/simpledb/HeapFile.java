package simpledb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 对于HeapPage有几个要注意的点
 * page的遍历是从 0开始的但是文档里面没有说明
 * 页面偏移量= pageNumber * pageSize
 * 元组偏移量  在当前找的Page上 偏移 headerSize+ tupleDesc.size()*tuple.number
 *
 * @see HeapPage#HeapPage
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
     * 这个返回的就是tableId
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
        FileChannel channel=openFiles.get(file.getAbsolutePath());
        openChannel(channel);
        channel=openFiles.get(file.getAbsolutePath());
        if(channel==null){
            throw new RuntimeException("chaneel打开失败");
        }
        int position=page.getId().pageNumber()*BufferPool.PAGE_SIZE;
        ByteBuffer dst=ByteBuffer.allocate(BufferPool.PAGE_SIZE);
        dst.put(page.getPageData());
        channel.write(dst,position);
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
        ArrayList<Page> result=new ArrayList<>();
        //从头Page开始找一个空闲的槽位
        for(int i=0;i<numPages();i++){
            PageId pageId=new HeapPageId(getId(),i);
            HeapPage heapPage= (HeapPage) Database.getBufferPool().getPage(tid,pageId,Permissions.READ_WRITE);
            if(heapPage.getNumEmptySlots()!=0){
                heapPage.insertTuple(t);
                result.add(heapPage);
                //todo 不确定更新操作是否需要反映到磁盘上
             //   writePage(heapPage);
                return result;
            }
        }
        //当前的heapFile上的所有的Page都满了要创建一个新的页
        HeapPageId pageId=new HeapPageId(getId(),numPages());
        numPage++;
        HeapPage heapPage= new HeapPage(pageId,HeapPage.createEmptyPageData());
        writePage(heapPage);
        HeapPage newPage= (HeapPage) Database.getBufferPool().getPage(tid,pageId,Permissions.READ_WRITE);
      //  t.setRecordId(new RecordId(pageId,0));  // You will need to ensure that the RecordID in the tuple is updated correctly.在HeapPage里面确定
        newPage.insertTuple(t);
        result.add(newPage);

        return result;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        HeapPage heapPage= (HeapPage) Database.getBufferPool().getPage(tid,t.getRecordId().getPageId(),Permissions.READ_WRITE);
            heapPage.deleteTuple(t);
            //todo 不确定这一个更新操作是否需要writePage操作 在操作指南中没要求flush到磁盘上
        return heapPage;
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

            }else if(pageIndex<pageSize-1){
                pageIndex++;
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

