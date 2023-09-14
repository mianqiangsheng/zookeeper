package com.msb.zookeeper.distributed_lock.InterprocessLock;


import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.ArrayList;
import java.util.List;

public class InterprocessLock {

    private final static CuratorFramework zkClient = getZkClient();

    public static void main(String[] args) throws Exception {
        /**
         * 分布式可重入公平排它锁
         */
//        String lockPath = "/lock";
//        InterProcessMutex lock = new InterProcessMutex(zkClient, lockPath);
//        //模拟3个线程抢锁
//        for (int i = 0; i < 3; i++) {
//            new Thread(new ReentrantThread(i, lock)).start();
//        }

        /**
         * 分布式不可重入排它锁
         */
//        String lockPath = "/lock";
//        InterProcessSemaphoreMutex lock = new InterProcessSemaphoreMutex(zkClient, lockPath);
//        //模拟3个线程抢锁
//        for (int i = 0; i < 3; i++) {
//            new Thread(new NonReentrantThread(i, lock)).start();
//        }

        /**
         * 分布式可重入公平读写锁
         */
        String lockPath1 = "/lock";
        InterProcessReadWriteLock interProcessReadWriteLock = new InterProcessReadWriteLock(zkClient,lockPath1);
        for (int i = 0; i < 2; i++) {
            new Thread(new ReadLockClient("Read"+i, interProcessReadWriteLock)).start();
        }
        for (int i = 0; i < 2; i++) {
            new Thread(new WriteLockClient("Write"+i, interProcessReadWriteLock)).start();
        }
        /**
         * 遍历指定父节点下的所有子节点
         */
//        List<String> nodes = getNode(zkClient,"/");
//        System.out.println(Arrays.toString(nodes.toArray()));
    }


    static class ReentrantThread implements Runnable {
        private String threadFlag;
        private InterProcessMutex lock;

        public ReentrantThread(String threadFlag, InterProcessMutex lock) {
            this.threadFlag = threadFlag;
            this.lock = lock;
        }

        @Override
        public void run() {
            try {
                lock.acquire();
                System.out.println("第" + threadFlag + "线程获取到了锁");
                //等到1秒后释放锁
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    lock.release();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    static class NonReentrantThread implements Runnable {
        private String threadFlag;
        private InterProcessSemaphoreMutex lock;

        public NonReentrantThread(String threadFlag, InterProcessSemaphoreMutex lock) {
            this.threadFlag = threadFlag;
            this.lock = lock;
        }

        @Override
        public void run() {
            try {
                lock.acquire();
                System.out.println("第" + threadFlag + "线程获取到了锁");
                //等到1秒后释放锁
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    lock.release();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    static class WriteLockClient implements Runnable {
        private String threadFlag;
        private InterProcessReadWriteLock lock;

        public WriteLockClient(String threadFlag, InterProcessReadWriteLock lock) {
            this.threadFlag = threadFlag;
            this.lock = lock;
        }

        @Override
        public void run() {
            InterProcessMutex interProcessMutex = null;
            try {
                interProcessMutex = lock.writeLock();
                interProcessMutex.acquire();
                System.out.println("第" + threadFlag + "线程获取到了锁");
                //等到1秒后释放锁
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (interProcessMutex != null){
                        interProcessMutex.release();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    static class ReadLockClient implements Runnable {
        private String threadFlag;
        private InterProcessReadWriteLock lock;

        public ReadLockClient(String threadFlag, InterProcessReadWriteLock lock) {
            this.threadFlag = threadFlag;
            this.lock = lock;
        }

        @Override
        public void run() {
            InterProcessMutex interProcessMutex = null;
            try {
                interProcessMutex = lock.readLock();
                interProcessMutex.acquire();
                System.out.println("第" + threadFlag + "线程获取到了锁");
                //等到1秒后释放锁
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (interProcessMutex != null){
                        interProcessMutex.release();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static CuratorFramework getZkClient() {
        /**
         * 取消sasl认证
         */
        System.setProperty("zookeeper.sasl.client", "false");
        String zkServerAddress = "10.10.103.221:2181,10.10.103.221:2182,10.10.103.224:2181";
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3, 5000);
        CuratorFramework zkClient = CuratorFrameworkFactory.builder()
                .connectString(zkServerAddress)
                .sessionTimeoutMs(10000)
                .connectionTimeoutMs(10000)
                .retryPolicy(retryPolicy)
                .build();
        zkClient.start();
        return zkClient;
    }

    public static List<String> getNode(CuratorFramework zkClient, String parentNode) throws Exception {

        List<String> res = new ArrayList<>();
        List<String> tmpList = zkClient.getChildren().forPath(parentNode);
        for (String tmp : tmpList) {
            String childNode = parentNode.equals("/") ? parentNode + tmp : parentNode + "/" + tmp;
            res.add(childNode);
            getNode(zkClient,childNode);
        }

        return res;
    }
}