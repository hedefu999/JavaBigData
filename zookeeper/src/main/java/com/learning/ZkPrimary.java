package com.learning;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZkPrimary {
    private static final Logger logger = LoggerFactory.getLogger(ZkPrimary.class);
    public static final String connectString = "127.0.0.1:2181";
    public static final String rootPath = "/zklearn";
    static CountDownLatch countDownLatch = new CountDownLatch(1);


    public static ZooKeeper getZKConnection(Watcher watcher) throws Exception{
        ZooKeeper zooKeeper = new ZooKeeper(connectString, 3000, watcher, false);
        countDownLatch.await();
        return zooKeeper;
    }

    static class Basic{
        static CountDownLatch countDownLatch = new CountDownLatch(1);
        static ZooKeeper getZKConnection() throws Exception{
            /*
            - 创建ZK连接
             */
            ZooKeeper zooKeeper = new ZooKeeper(connectString, 3000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    logger.info("接收到事件: {}", event.toString());
                    if (Event.KeeperState.SyncConnected == event.getState()){
                        countDownLatch.countDown();
                    }
                }
            }, false);
            countDownLatch.await();
            //密码已加密
            long sessionId = zooKeeper.getSessionId();
            byte[] sessionPasswd = zooKeeper.getSessionPasswd();
            logger.info("zk连接已建立: sessionId = {}, sessionPsw = {}",sessionId , new String(sessionPasswd));
            /*
            - 将 sessionId, sessionPassword 用于 新建 ZK 连接
            如果使用了无效的 session id和password会提示 expire
            new ZooKeeper(connectString, 5000, null, sessionId, sessionPasswd);
             */
            return zooKeeper;
        }
        /*
        - zkClient create API
         */
        static void createAPI(ZooKeeper zooKeeper) throws Exception{
            /*
            - Sync Version
            //返回result: /zklearn   path不能写不存在父路径的多级路径
            String result = zooKeeper.create("/zklearn", "data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            //返回result: /zklearn0000000119 临时节点，多次运行main方法不会报 NodeExistsException
            String result = zooKeeper.create("/zklearn", "data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
             */

            /*
            - Async Version 此时create方法无返回
             */
            zooKeeper.create("/zkLearn", "data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                    new AsyncCallback.StringCallback() {
                        @Override
                        public void processResult(int rc, String path, Object ctx, String name) {
                            //processResult: rc=0,path=/zkLearn,ctx=context string: 12,name=/zkLearn
                            logger.info("processResult: rc={},path={},ctx={},name={}",rc,path,ctx,name);
                        }
                    }, "context string: 12");
            //不等一下看不到 processResult 回调
            Thread.sleep( 3000);
        }
        /*
        - delete API
         */
        static void deleteAPI(ZooKeeper zooKeeper){

        }
        /*
        - get API
        getChildren
         */
        static class AWatcher implements Watcher{
            private ZooKeeper zkConnection = null;

            public AWatcher(ZooKeeper zkConnection) {
                this.zkConnection = zkConnection;
            }

            @Override
            public void process(WatchedEvent event) {
                logger.info("got event: path = {}, type = {}, state = {} ", event.getPath(), event.getType(), event.getState());
                if (Event.KeeperState.SyncConnected == event.getState()) {
                    if (Event.EventType.None == event.getType() && null == event.getPath()) {
                        logger.info("获取连接后得到第一个空事件");
                        ZkPrimary.countDownLatch.countDown();
                    } else if (Event.EventType.NodeChildrenChanged == event.getType()) {
                        List<String> children = null;
                        try {
                            children = zkConnection.getChildren(event.getPath(), true);
                        } catch (Exception e) {
                            logger.error("监听事件发生异常：", e);
                        }
                        logger.info("获取到children子节点：{}", children);
                    }
                }
            }
        }
        static void retrieveAPI() throws Exception{
            //在研究get api前，要创建一些永久节点
            //String s = zkConnection.create("/zklearn", "tutorial".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            //logger.info("同步创建节点，结果：{}", s);
            ZooKeeper zkConnection = null;
            zkConnection = ZkPrimary.getZKConnection(new AWatcher(zkConnection));
            zkConnection.create("/zklearn/c1","c1test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            zkConnection.create("/zklearn/c3","c3test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            List<String> children = zkConnection.getChildren(rootPath, true);
            logger.info("get children simply: {}", children);
            Thread.sleep(3000);

        }
        public static void main(String[] args) throws Exception{
            retrieveAPI();

        }
    }
    public static void main(String[] args) {
        System.out.println("Hello world!");
    }
}