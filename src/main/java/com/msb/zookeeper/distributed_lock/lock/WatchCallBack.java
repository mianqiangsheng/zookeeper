package com.msb.zookeeper.distributed_lock.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 分布式锁的实现（一）队列式的公平锁
 * @author: 马士兵教育
 * @create: 2019-09-20 21:26
 */
public class WatchCallBack   implements Watcher  ,AsyncCallback.StringCallback ,AsyncCallback.Children2Callback ,AsyncCallback.StatCallback {

    ZooKeeper zk ;
    String threadName;
    CountDownLatch cc = new CountDownLatch(1);
    String pathName;

    public String getPathName() {
        return pathName;
    }

    public void setPathName(String pathName) {
        this.pathName = pathName;
    }

    public String getThreadName() {
        return threadName;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    public ZooKeeper getZk() {
        return zk;
    }

    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }

    public void tryLock(){
        try {

            System.out.println(threadName + "  create....");
//            if(zk.getData("/"))

            /**
             * 如果再次获得锁，发现当前持有的锁的线程就是自己，则直接放行（根据查看锁目录下的数据是否是当前线程名）
             */
            try {
                byte[] data = zk.getData("/", false, new Stat());
                if (threadName.equals(new String(data)))
                    return;
            } catch (KeeperException e) {
                e.printStackTrace();
            }
            zk.create("/lock",threadName.getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL,this,"abc");

            cc.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void unLock(){
        try {
            zk.delete(pathName,-1);
            System.out.println(threadName + " over work....");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void process(WatchedEvent event) {


        //如果第一个哥们，那个锁释放了，其实只有第二个收到了回调事件！！
        //如果，不是第一个哥们，某一个，挂了，也能造成他后边的收到这个通知，从而让他后边那个跟去watch挂掉这个哥们前边的。。。
        switch (event.getType()) {
            case None:
                break;
            case NodeCreated:
                break;
            case NodeDeleted:
                zk.getChildren("/",false,this ,"sdf");
                break;
            case NodeDataChanged:
                break;
            case NodeChildrenChanged:
                break;
        }

    }

    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
        if(name != null ){
            System.out.println(threadName  +"  create node : " +  name );
            pathName =  name ;
            zk.getChildren("/",false,this ,"sdf");
        }

    }

    //getChildren  call back
    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {

        //一定能看到自己前边的。。

//        System.out.println(threadName+"look locks.....");
//        for (String child : children) {
//            System.out.println(child);
//        }

        /**
         * 获取的节点信息不是排序的，需要手动排序一下
         */
        Collections.sort(children);
        int i = children.indexOf(pathName.substring(1));


        //是不是第一个
        if(i == 0){
            //yes
            System.out.println(threadName +" i am first....");
            try {
                /**
                 * 设置锁根目录数据为当前排第一的线程名有2个作用
                 * 1、避免获得锁的线程快速释放锁，而下一个节点的线程还未完成监测的zookeeper逻辑，导致下一个节点的线程永远没机会获得锁
                 * 2、埋入可重入锁的判断条件
                 */
                zk.setData("/",threadName.getBytes(),-1);
                cc.countDown();

            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }else{
            //no
            zk.exists("/"+children.get(i-1),this,this,"sdf");
        }

    }

    /**
     * 设置zk.exists()的callback函数，
     * 严谨的话应该也要对zk.exists("/"+children.get(i-1),this,this,"sdf");可能失败的情况做出反应
     * @param rc
     * @param path
     * @param ctx
     * @param stat
     */
    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
        //偷懒
    }
}
