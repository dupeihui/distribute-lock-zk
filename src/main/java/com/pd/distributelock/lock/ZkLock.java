package com.pd.distributelock.lock;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

@Slf4j
public class ZkLock implements Watcher,AutoCloseable {

    private ZooKeeper zooKeeper;
    private String businessName;
    private String znode;

    public ZkLock(String connectString, String businessName) throws IOException {
        this.zooKeeper = new ZooKeeper(connectString, 30000, this);
        this.businessName = businessName;
    }

    /**
     * 获得分布式锁
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public boolean getLock() throws KeeperException, InterruptedException {
        //创建业务根节点
        Stat stat = zooKeeper.exists("/" + businessName, false);
        if (stat == null) {
            zooKeeper.create("/" + businessName, businessName.getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        //创建瞬时有序节点，/order/order_0000000001
        znode = zooKeeper.create("/" + businessName + "/" + businessName + "_", businessName.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        znode = znode.substring(znode.lastIndexOf("/")+1);
        log.info("瞬时有序节点：" + znode);

        List<String> childrenNodes = zooKeeper.getChildren("/" + businessName, false);
        Collections.sort(childrenNodes);

        String firstNode = childrenNodes.get(0);
        //创建的节点不是序号最小节点，则监听前一个节点
        if (!firstNode.equals(znode)) {
            String previousNode = firstNode;
            for (String node : childrenNodes) {
                //寻找前一个节点
                if (!znode.equals(node)) {
                    previousNode = node;
                } else {
                    zooKeeper.exists("/" + businessName + "/" + previousNode, true);
                    break;
                }
            }
            //等待线程释放锁
            synchronized (this) {
                wait();
            }
        }
        //创建的节点是序号最小节点，获得锁
        return true;
    }


    @Override
    public void process(WatchedEvent watchedEvent) {
        //监听前一个节点删除事件，监听到事件发生，则唤醒线程
        if (watchedEvent.getType() == Event.EventType.NodeDeleted){
            synchronized (this){
                notify();
            }
        }
    }

    @Override
    public void close() throws Exception {
        zooKeeper.delete("/"+businessName+"/"+znode,-1);
        zooKeeper.close();
        log.info("我释放了锁");
    }
}
