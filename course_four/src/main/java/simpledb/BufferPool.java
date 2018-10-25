package simpledb;

import simpledb.util.LRUCache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */



    private Permissions permissions;

    private  final Map<PageId,Page> cache;

    private LockManager lockManager;



    public BufferPool(int numPages) {
        // some code goes here

        this.cache=new LRUCache<>(numPages);
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here


       this.permissions=perm;
           Page page= cache.get(pid);
           if(page!=null){
               return page;
           }else{
               DbFile table = Database.getCatalog().getDbFile(pid.getTableId());
               try {
                   page=new HeapPage((HeapPageId) pid, table.readPage(pid).getPageData());
                   cache.put(pid,page);
                   return page;
               } catch (IOException e) {
                   e.printStackTrace();
               }
           }
        return null;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for proj1
        lockManager.unloock(tid,pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        lockManager.releaseTidLocks(tid);
        // not necessary for proj1
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for proj1
        return lockManager.holdLock(tid,p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for proj1
        lockManager.releaseTidLocks(tid);
        if(commit){
            flushPages(tid);
        }else{
            //rollback
            List<PageId> list= lockManager.getAllPageIdByTid(tid);
            list.forEach(pid->{
                try {
                    HeapPage heapPage =(HeapPage)getPage(tid,pid,Permissions.READ_ONLY);
                    heapPage=   heapPage.getBeforeImage();
                    DbFile dbFile= Database.getCatalog().getDbFile(pid.getTableId());
                    dbFile.writePage(heapPage);
                } catch (TransactionAbortedException e) {
                    e.printStackTrace();
                } catch (DbException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            list.forEach(this::discardPage);
        }


    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
           DbFile table= Database.getCatalog().getDbFile(tableId);
        ArrayList<Page> result= table.insertTuple(tid,t);
            result.forEach(page->page.markDirty(true,tid));
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        DbFile table= Database.getCatalog().getDbFile(t.getRecordId().getPageId().getTableId());
           Page page= table.deleteTuple(tid,t);
           page.markDirty(true,tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     *     todo just used by  test
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for proj1
        Map<PageId,Page> temp=new HashMap<>();

        cache.forEach((k,v)->{
                temp.put(k,v);
        });
        temp.forEach((k,v)->{
            try {
                flushPage(k);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });


    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
	// not necessary for proj1

      cache.remove(pid);

    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     *
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for proj1
        try {
            Page page=  getPage(null,pid,permissions);
            if(page.isDirty()!=null){
                DbFile dbFile= Database.getCatalog().getDbFile(pid.getTableId());
                dbFile.writePage(page);
                page.markDirty(false,null);
                discardPage(pid);
            }
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } catch (DbException e) {
            e.printStackTrace();
        }

    }

   private ThreadPoolExecutor threadPoolExecutor=new ThreadPoolExecutor(5, 20, 30, TimeUnit.MILLISECONDS,
       new ArrayBlockingQueue<>(20), new ThreadFactory() {
       @Override
       public Thread newThread(Runnable r) {
           return new Thread("flush page thread");
       }
   });

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
        List<PageId> pageIds= lockManager.getAllPageIdByTid(tid);

            pageIds.forEach(pageId-> {
                try {
                  HeapPage heapPage=(HeapPage) Database.getBufferPool().getPage(tid,pageId,Permissions.READ_ONLY);

                  if(heapPage.isDirty()!=null){
                      threadPoolExecutor.execute(()->{
                          try {
                              flushPage(pageId);
                          } catch (IOException e) {
                              e.printStackTrace();
                          }
                      });

                  }
                } catch (TransactionAbortedException e) {
                    e.printStackTrace();
                } catch (DbException e) {
                    e.printStackTrace();
                }

            });



    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for proj1
    }

}
