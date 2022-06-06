package com.learning;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZkPrimary {
    private static final Logger logger = LoggerFactory.getLogger(ZkPrimary.class);
    public static final String connectString = "127.0.0.1:2181";
    public static final String rootPath = "/zklearn";
    static CountDownLatch countDownLatch = new CountDownLatch(1);

    /**
     * 一个通用的Watcher
     * watcher要反复注册使用，所以收到一次通知时还要注册下
     */
    static class CommonWatcher implements Watcher{
        private ZooKeeper zkConnection = null;

        public void setZkConnection(ZooKeeper zkConnection) {
            this.zkConnection = zkConnection;
        }

        @Override
        public void process(WatchedEvent event) {
            logger.info("got event: path = {}, type = {}, state = {} ", event.getPath(), event.getType(), event.getState());
            try {
                if (Event.KeeperState.SyncConnected == event.getState()) {
                    if (Event.EventType.None == event.getType() && null == event.getPath()) {
                        logger.info("获取连接后得到第一个空事件");
                        ZkPrimary.countDownLatch.countDown();
                    } else if (Event.EventType.NodeChildrenChanged == event.getType()) {
                        zkConnection.exists(event.getPath(), true);
                        List<String> children = zkConnection.getChildren(event.getPath(), true);
                        logger.info("获取到children子节点：{}", children);
                    } else if (Event.EventType.NodeDataChanged == event.getType()){
                        zkConnection.exists(event.getPath(), true);
                        byte[] data = zkConnection.getData(event.getPath(), true, new Stat());
                        logger.info("eventType = NodeDataChanged, retrieve data = {}", new String(data));
                    } else if (Event.EventType.NodeCreated == event.getType()){
                        zkConnection.exists(event.getPath(),true);
                    } else if (Event.EventType.NodeDeleted == event.getType()){
                        zkConnection.exists(event.getPath(), true);
                    }
                }
            } catch (Exception e){
                logger.error("监听事件发生异常：", e);
            }
        }
    }

    public static ZooKeeper getZKConnection(Watcher watcher) throws Exception{
        ZooKeeper zooKeeper = new ZooKeeper(connectString, 3000, watcher, false);
        countDownLatch.await();
        return zooKeeper;
    }

    static class ZkClientAPILearn{
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
        getChildren API 的同步版本
         */
        static void retrieveAPI() throws Exception{
            //在研究get api前，要创建一些永久节点
            //String s = zkConnection.create("/zklearn", "tutorial".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            //logger.info("同步创建节点，结果：{}", s);
            ZooKeeper zkConnection = null;
            CommonWatcher commonWatcher = new CommonWatcher();
            zkConnection = ZkPrimary.getZKConnection(commonWatcher);
            commonWatcher.setZkConnection(zkConnection);
            zkConnection.create("/zklearn/c1","c1test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            zkConnection.create("/zklearn/c3","c3test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            List<String> children = zkConnection.getChildren(rootPath, true);
            logger.info("get children simply: {}", children);
        }

        /*
        getChildren API 的异步版本
         */
        static class ZookeeperGetChildrenAsyncAPI implements Watcher{
            private static CountDownLatch countDownLatch = new CountDownLatch(1);
            private static ZooKeeper zooKeeper = null;
            @Override
            public void process(WatchedEvent event) {
                logger.info("got event: path = {}, type = {}, state = {} ", event.getPath(), event.getType(), event.getState());
                if (Event.EventType.None == event.getType() && null == event.getPath()){
                    logger.info("空类型事件");
                    countDownLatch.countDown();
                }else if (Event.EventType.NodeChildrenChanged == event.getType()){
                    logger.info("子节点发生变化");
                    try {
                        zooKeeper.getChildren(event.getPath(), true);
                    } catch (Exception e) {
                        logger.error("发生错误：", e);
                    }
                }
            }

            public static void main(String[] args) throws Exception{
                ZooKeeper zooKeeper = new ZooKeeper(connectString, 5000, new ZookeeperGetChildrenAsyncAPI());
                countDownLatch.await();
                zooKeeper.create(rootPath+"/c2", "c2".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                zooKeeper.create(rootPath+"/c1", "c1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                zooKeeper.getChildren(rootPath, true, new AsyncCallback.Children2Callback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                        logger.info("processResult: rc = {}, path = {}, ctx = {}, children = {}, stat = {}", rc, path, ctx, children, stat);
                    }
                }, null);
                Thread.sleep(30000);
            }

        }

        /*
        getData API
         */
        static void getDataSync(ZooKeeper zooKeeper) throws Exception{
            String path = rootPath + "/c4";
            zooKeeper.create(path, "c4d".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            //引用传递一个stat变量，后面能拿到更新后的结果
            Stat stat = new Stat();
            //getData boolean watch这里传入true，CommonWatcher那边才能收到NodeDataChanged事件
            byte[] data = zooKeeper.getData(path, true, stat);
            logger.info("getData, return {}, stat = {},{}", new String(data), stat.getMzxid(), stat.getVersion());
            //version = -1 表示针对所有version节点数据进行修改
            Stat setStat = zooKeeper.setData(path, "c4".getBytes(), -1);
            logger.info("setData result: stat = {},{}", setStat.getMzxid(), setStat.getVersion());
        }

        static void getDataAsync(ZooKeeper zooKeeper) throws Exception{
            String path = rootPath + "/c4";
            zooKeeper.create(path, "c4".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            zooKeeper.getData(path, true, new AsyncCallback.DataCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                    logger.info("getDataAsync processResult: rc={},path={},ctx={},data={},stat={},{}", rc,path,ctx,new String(data),stat.getMzxid(),stat.getVersion());
                }
            }, null);
            zooKeeper.setData(path, "c4".getBytes(), -1);
        }
        /*
        exists API
         */
        static void existsAPI(ZooKeeper zooKeeper) throws Exception{
            //Stat rootPathExists = zooKeeper.exists(rootPath, true);
            //if (rootPathExists != null){
            //    logger.info("root path exists: {},{}", rootPathExists.getMzxid(), rootPathExists.getVersion());
            //    List<String> children = zooKeeper.getChildren(rootPath, true);
            //    logger.info("get children: {}", children);
            //}
            String path = rootPath + "/c4";
            String path2 = rootPath + "/c5";
            Stat exists = zooKeeper.exists(path, true);
            if (exists != null){
                logger.info("exists api: stat = {},{}", exists.getMzxid(), exists.getVersion());
            }
            zooKeeper.create(path, "c4".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            zooKeeper.setData(path, "c4d".getBytes(), -1);

            //如果想让c5的创建触发事件，至少要调用一个带 boolean watch 的API
            zooKeeper.exists(path2, true);
            zooKeeper.create(path2, "c5".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            //zooKeeper.exists(path2, true); 缺少这一行 c5 的删除事件不会触发！！！
            zooKeeper.delete(path2, -1);
        }
        /*
        ACL API
         */
        static void createNodeWithACL(ZooKeeper zooKeeper) throws Exception{
            String authPath = "/zklearn_acl";
            //digest 模式权限
            zooKeeper.addAuthInfo("digest", "userA:passCodeA".getBytes());
            zooKeeper.create(authPath, "init".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.EPHEMERAL);
        }

        public static void main(String[] args) throws Exception{
            ZooKeeper zooKeeper = null;
            CommonWatcher commonWatcher = new CommonWatcher();
            zooKeeper = ZkPrimary.getZKConnection(commonWatcher);
            commonWatcher.setZkConnection(zooKeeper);

            //getDataSync(zooKeeper);
            //getDataAsync(zooKeeper);
            //existsAPI(zooKeeper);
            createNodeWithACL(zooKeeper);
            Thread.sleep(30000);

        }
    }
    public static void main(String[] args) {
        System.out.println("Hello world!");
    }
}