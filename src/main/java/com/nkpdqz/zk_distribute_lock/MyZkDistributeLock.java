package com.nkpdqz.zk_distribute_lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class MyZkDistributeLock {

    private String urlAndPort;
    private Integer timeOut;
    private ZooKeeper zkClient;
    private static final String LOCK_ROOT_PATH = "/locks";
    private static final String LOCK_NODE_NAME = "lock_";
    private String lockPath;

    private Watcher watcher = new Watcher() {
        public void process(WatchedEvent watchedEvent) {
            System.out.println(watchedEvent.getPath()+" 锁释放");
            synchronized (this){
                notifyAll();
            }
        }
    };

    public MyZkDistributeLock(String urlAndPort,Integer timeOut) throws IOException {
        this.urlAndPort = urlAndPort;
        this.timeOut = timeOut;
        zkClient = new ZooKeeper(urlAndPort, timeOut, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getState()==Event.KeeperState.Disconnected)
                    System.out.println("失去连接");
            }
        });
    }

    public void acquireLock() throws KeeperException, InterruptedException {
        createLock();
        attemptLock();
    }

    //尝试获取锁
    private void attemptLock() throws KeeperException, InterruptedException {
        //获取Lock子节点，按照序号排序
        List<String> lockPaths = zkClient.getChildren(LOCK_ROOT_PATH,false);
        Collections.sort(lockPaths);
        int index = lockPaths.indexOf(lockPath.substring(LOCK_NODE_NAME.length()));
        if (index==0){
            System.out.println(Thread.currentThread().getName()+"获取锁,lockPath="+lockPath);
            return;
        }else {
            //监控前一个节点
            String preLockPath = lockPaths.get(index - 1);
            Stat stat = zkClient.exists(LOCK_ROOT_PATH + "/" + preLockPath, watcher);
            //如果前一个节点不存在了，重新获取锁
            if (stat != null) {
                System.out.println("等待当前锁释放，preLockPath: " + preLockPath);
                synchronized (watcher) {
                    watcher.wait();
                }
            }
            attemptLock();

        }
    }

    //创建锁节点
    private void createLock() throws KeeperException, InterruptedException {
        //首先创建根节点（若不存在）
        Stat stat = zkClient.exists(LOCK_ROOT_PATH,false);
        if (stat==null){
            zkClient.create(LOCK_ROOT_PATH,new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        }
        lockPath = zkClient.create(LOCK_ROOT_PATH+"/"+LOCK_NODE_NAME,Thread.currentThread().getName().getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(Thread.currentThread().getName() + " 锁创建： "+lockPath );
    }

    public void releaseLock() throws KeeperException, InterruptedException {
        zkClient.delete(lockPath,-1);
        zkClient.close();
        System.out.println("锁释放： "+lockPath);
    }
}
