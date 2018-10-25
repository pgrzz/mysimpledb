package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class LockManager {

    private Map<PageId,List<PageStatus>>  pageLockMap=new ConcurrentHashMap<>();

    private Map<TransactionId,PageId> tx=new ConcurrentHashMap<>();

    public ReentrantLock lock=new ReentrantLock(true);


    public Condition condition=lock.newCondition();




    public  boolean  lock(TransactionId tid, PageId pid, Permissions perm) throws TimeoutException{
        //todo 这里有一个优化 就是锁list可以保存一个最大状态节点这样就节省了遍历的时间

        //判断当前page的状态
        List<PageStatus> locks= pageLockMap.get(pid);

        if(locks!=null && locks.size()>0){
            //page 上已经有锁了 看是什么等级的锁 读锁则直接分配读锁 写锁则等待 同一时刻只会有一把X 可以多把S
            PageStatus pageStatus;
            pageStatus=  locks.stream().filter(var->var.perm==Permissions.READ_WRITE).findFirst().orElse(null);
            if(pageStatus!=null && pageStatus.perm==Permissions.READ_WRITE){
                //判断是不是自己的
                 if(pageStatus.tid.getId()==tid.getId()){
                     //自己的事务
                     return true;

                 }else{
                     // todo 外部对这个 tid的重试逻辑 ,等到 其他tid的X 锁得到释放加入参tid X 锁或者S 锁这样避免了 X锁被饿死
                     addWaitStatus(tid,pid);
                    return false;
                     //等待列表等待
                 }
            }else{
                //page上只有S锁

                pageStatus=locks.stream().filter(var->var.tid.equals(tid)).findFirst().orElse(null);
                if(pageStatus!=null && pageStatus.perm==Permissions.READ_ONLY){
                    //page上有自己的S锁
                    return true;
                }else{
                    lockSorX(tid,pid, perm);
                    removeWaitStatus(tid);
                    return true;
                }
            }
        }else{
            lockSorX(tid,pid, perm);
            removeWaitStatus(tid);
            return true;
        }



    }


    boolean unloock(TransactionId tid, PageId pid){

        List<PageStatus> list=    pageLockMap.get(pid);
        if(list==null ||list.size()<1){return false;}

        PageStatus pageStatus= list.stream().filter(a->a.tid==tid).findFirst().orElse(null);
                return list.remove(pageStatus);
    }

    boolean isWait(TransactionId tid){
        return tx.containsKey(tid);
    }

    void addWaitStatus(TransactionId tid, PageId pid){
        tx.put(tid,pid);
    }
    void removeWaitStatus(TransactionId tid){
        tx.remove(tid);

    }

    void  lockSorX(TransactionId tid, PageId pid,Permissions permissions){
        List<PageStatus> list=  pageLockMap.get(pid);
            if(list==null){
                list=new CopyOnWriteArrayList<>();
            }
            list.add(new PageStatus(tid,permissions));
        pageLockMap.put(pid,list);
    }



    boolean holdLock(TransactionId tid, PageId pid){
        List<PageStatus> list=    pageLockMap.get(pid);
        if(list==null ||list.size()<1){return false;}

        PageStatus pageStatus= list.stream().filter(a->a.tid==tid).findFirst().orElse(null);
        return pageStatus!=null;
    }


    void releaseTidLocks(TransactionId tid){
        List<PageId> pageIds= getAllPageIdByTid(tid);
        pageIds.forEach(a->unloock(tid,a));
    }

    List<PageId> getAllPageIdByTid(TransactionId tid){
        List<PageId> list=new LinkedList<>();
        pageLockMap.forEach((k,v)->{
            long o=  v.stream().filter(status->status.tid.equals(tid)).count();
              if(o>0){
                  list.add(k);
              }
        });
        return list;

    }


    class PageStatus{
        TransactionId tid;
        Permissions perm;

        public PageStatus(TransactionId tid, Permissions perm) {
            this.tid = tid;
            this.perm = perm;
        }


    }

    class  TimeoutException extends RuntimeException{

    }

}
