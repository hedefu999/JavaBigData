package com.learning;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.EnsurePath;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
        - delete API，见ACL API
         */

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
            //此时其他Zookeeper访问会报 KeeperErrorCode=NoAuth for /zklearn_acl
        }
        /*
        delete API的ACL API比较特殊
         */
        static void deleteAPIACL() throws Exception{
            String authPath = "/zklearn_acl";
            String authPath2 = "/zklearn_acl/child";
            byte[] token = "token2333".getBytes();
            String schema = "digest";

            ZooKeeper zkConn0 = getZKConnection();
            zkConn0.addAuthInfo(schema, token);
            zkConn0.create(authPath, "init".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
            zkConn0.create(authPath2, "init".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.EPHEMERAL);
            ZooKeeper zkConn1 = getZKConnection();
            try {
                zkConn1.delete(authPath2, -1);//将抛异常
            }catch (Exception e){
                logger.info("删除节点发生异常：{}", e.getMessage()); //KeeperErrorCode = NoAuth for /zklearn_acl/child
                zkConn1.addAuthInfo(schema, token);
                zkConn1.delete(authPath2, -1);
            }
            ZooKeeper zkConn2 = getZKConnection();
            zkConn2.delete(authPath, -1);//没有权限也可以删
        }

        public static void main(String[] args) throws Exception{
            ZooKeeper zooKeeper = null;
            CommonWatcher commonWatcher = new CommonWatcher();
            zooKeeper = ZkPrimary.getZKConnection(commonWatcher);
            commonWatcher.setZkConnection(zooKeeper);

            //getDataSync(zooKeeper);
            //getDataAsync(zooKeeper);
            //existsAPI(zooKeeper);
            deleteAPIACL();
            Thread.sleep(30000);

        }
    }

    /**
     * ZkClient是Github上开源的一个Zookeeper客户端，由Datameer的工程师Stefan Groschupf和Peter Voss开发
     * 听说提供了 session超时重连，watcher反复注册的功能
     */
    static class OpenSourceZkClient{
        static ZkClient zkClient = new ZkClient(connectString, 5000);
        static void useIZkConnection(){
            zkClient.create(rootPath+"/zkclient", "testZkClient", CreateMode.EPHEMERAL);
            zkClient.createPersistent(rootPath+"/zkclient/hello", true);
            zkClient.deleteRecursive(rootPath);
            //返回子节点的相对路径：不包含 rootPath
            List<String> children = zkClient.getChildren(rootPath);
        }
        /*
        使用Listener 监听子节点及节点本身的增删
         */
        static void getChildrenWithListener() throws Exception{
            zkClient.subscribeChildChanges(rootPath+"2", new IZkChildListener() {
                @Override// currentChilds 可为null、emptyList
                public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                    logger.info("handle children changes: 父节点全路径 parentPath={}, 变化后的子节点相对路径 currentChildren={}", parentPath, currentChilds);
                }
            });
            //handle children changes: parentPath=/zklearn, currentChildren=[c3, c6, c2]
            //节点本身创建得到 currentChildren=[]
            //带有一个子节点的父节点被递归删除时会受到两次 parentPath=/zklearn2,currentChildren=null
            zkClient.deleteRecursive(rootPath+"2");
            Thread.sleep(30000);
        }
        /*
        zkClient 获取节点数据
        */
        static void getData(){
            //returnNullIfPathNotExists=false（默认值），表示节点不存在时抛出异常
            //zkClient内能自动反序列化得到执行类型对象
            String data = zkClient.readData(rootPath + "/c5", false);

            Stat stat = new Stat();
            zkClient.readData(rootPath+"/c6", stat);
            //此时可以得到节点的stat信息
        }
        /*
        zkCLient 订阅节点本身data的变化
         */
        static void subscribeDataChanges(){
            zkClient.subscribeDataChanges(rootPath, new IZkDataListener() {
                @Override
                public void handleDataChange(String dataPath, Object data) throws Exception {
                    //订阅的节点的 内容、版本发生变化时都会收到通知，此时收到的dataPath是绝对路径，data是数据节点的最新内容
                }
                @Override //节点删除事件
                public void handleDataDeleted(String dataPath) throws Exception {
                    //dataPath 是被删除的节点的全路径
                }
            });
        }
        /*
        zkClient 写入数据+检测节点是否存在
         */
        static void writeData(){
            zkClient.writeData(rootPath, new Integer(1));
            zkClient.exists(rootPath);
        }

        public static void main(String[] args) throws Exception{
            getChildrenWithListener();
        }
    }

    /**
     * Curator 是Netflix开源的一套Zookeeper客户端框架，作者是Jrdan Zimmerman
     * Zookeeper代码的核心提交人Patrick Hunt称 Curator之于Zookeeper 相当于 Guava之于Java
     * Curator在Zookeeper原生API基础上进行包装，提供一套易用性和可读性更强的Fluent风格的客户端API框架
     * Curator中还提供了Zookeeper各种应用场景，如 Recipe 共享锁服务 Master选举机制 分布式计数器
     */
    static class OpenSourceCurator{
        public static final String CONN_STRING = "127.0.0.1:2181";
        public static final String ROOT_PATH = "/zklearn";
        static void createCurator() throws Exception{
            /*
            ExponentialBackoffRetry 重试策略：指数增长的重试，重试的时间间隔不断增大
             */
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
            //retryPolicy.allowRetry()
            /*
            retryPolicy 重试策略 默认有4种实现：ExponentialBackoffRetry,RetryNTimes,RetryOneTime,RetryUntilElapsed
            sessionTimeoutMs 会话超时时间 默认 60 000ms
            connectionTimeoutMs 连接创建超时时间 默认 15000ms
             */
            CuratorFramework curator = CuratorFrameworkFactory.newClient(CONN_STRING, 5000, 3000, retryPolicy);
            //new 完了并没有创建会话，还需要start！
            curator.start();
            Thread.sleep(3000);
        }
        static void fluentStyleSyncAPI()throws Exception{
            ExponentialBackoffRetry retry = new ExponentialBackoffRetry(1000, 3);
            CuratorFramework curator = CuratorFrameworkFactory.builder()
                    .connectString(connectString)
                    .sessionTimeoutMs(5000)
                    .retryPolicy(retry)
                    //使用Curator创建含隔离命名空间的会话（就是指定一个根目录）
                    .namespace("zklearn")
                    .build();
            curator.start();

            //创建节点，初始data为空.默认创建持久节点
            curator.create().forPath("/c4");
            curator.create()
                    //由于Zookeeper中所有非叶子节点必须是持久节点，所以curator自动创建的父节点都是持久节点
                    .creatingParentContainersIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath("/c5", "initData".getBytes());

            //此删除只能删除叶子节点
            curator.delete().forPath("/c4");
            //删除节点并递归删除所有其子节点
            curator.delete().deletingChildrenIfNeeded().forPath("/c5");
            //删除节点，强制指定版本进行删除
            curator.delete().withVersion(3).forPath("/c5");
            //强行删除一个节点：guaranteed()接口是一个保障措施，只要客户端会话有效，curator就会在后台持续进行删除，直到节点删除成功
            //客户端实际删除中可能会因为网络原因，导致删除失败，这个异常在某些场景中是致命的，如Master选举
            //curator引入了一种重试机制：调用guranteed()方法后，客户端碰到这类网络异常时，会记录下这次失败的删除操作，只要客户端会话有效，就会在后台反复重试，直至删除成功
            curator.delete().guaranteed().forPath("/c6");

            //getData Stat
            String path = "/zklearn/c6";
            curator.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(path, "init".getBytes());
            Stat stat = new Stat();
            //读取节点内容并获取该节点的Stat
            byte[] bytes = curator.getData().storingStatIn(stat).forPath(path);
            String data = new String(bytes);

            //update
            //withVersion 接口用于更新数据时进行CAS，version信息通常从一个旧的Stat中获取
            Stat newStat = curator.setData().withVersion(stat.getVersion()).forPath(path);
            int newVersion = newStat.getVersion();

            curator.delete()
                    .deletingChildrenIfNeeded()
                    .withVersion(stat.getVersion())
                    .forPath(path);
        }
        /*
        curator的异步接口 BackgroundCallback
         */
        static CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .sessionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .namespace("zklearn")
                .build();

        static void asyncCuratorAPI() throws Exception{
            String path = "/c6";
            CountDownLatch semaphore = new CountDownLatch(1);
            ExecutorService threadPool = Executors.newFixedThreadPool(2);

            client.start();
            logger.info("current thread: {}", Thread.currentThread().getName());
            BackgroundCallback backgroundCallback = new BackgroundCallback() {
                /*
                CuratorEvent.type有 CREATE DELETE EXISTS GET_DATA SET_DATA CHILDREN SYNC GET_ACL WATCHED CLOSING
                分别代表 对应API调用触发的事件 checkExists、getChildren、sync、getACL、usingWatcher/watched
                CuratorEvent.resultCode 定义在 KeeperException.Code类中，常见响应码 0(OK)、-4(ConnectionLoss)、-110(NodeExists)、-112(SessionExpired)
                 */
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                    //节点如果已存在，resultCode = -110
                    logger.info("{} event-resultCode = {}, event-type = {}",
                            Thread.currentThread().getName(), event.getResultCode(), event.getType());
                    semaphore.countDown();
                }
            };
            client.create().creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    //如果不传入Executor则会使用Zookeeper默认的EventThread
                    //在ZooKeeper中，所有异步通知事件处理都是由EventThread这个线程进行处理的（串行）,为避免复杂场景下的响应慢问题，可以传入专门的线程池
                    .inBackground(backgroundCallback, threadPool)
                    .forPath(path, "init".getBytes());

            semaphore.wait();
            threadPool.shutdown();
        }
        /*
        ZooKeeper的原生Watcher机制需要反复注册，比较繁琐。Curator能够自动为开发人员处理反复注册监听，简化原生API。
        Curator引入Cache实现对ZooKeeper服务端事件的监听。Cache是Curator中对事件监听的包装，其对事件的监听可以看做是本地缓存视图与远程ZooKeeper视图的对比过程
        Cache分为两类监听类型：NodeCahce 节点监听 + 子节点监听
         */
        static void curatorCacheAPI()throws Exception{
            String path = "/zklearn/c7";
            client.start();

            /*
            NodeCache 的使用
             */
            NodeCache nodeCache = new NodeCache(client, path, false);
            nodeCache.start(true);
            //curator可以监听节点变化（节点创建、节点data变化，但节点删除无法监听）
            nodeCache.getListenable()
                    .addListener(new NodeCacheListener() {
                        @Override
                        public void nodeChanged() throws Exception {
                            logger.info("node data changed! {}", new String(nodeCache.getCurrentData().getData()));
                        }
                    });
            client.setData().forPath(path, "change".getBytes());
            Thread.sleep(3000);
            client.delete().deletingChildrenIfNeeded().forPath(path);
            Thread.sleep(3000);

            /*
            PathChildCache 的使用
            PathChildCache用于监听指定节点及其子节点的变化情况
            PathChildrenCacheEvent.Type 枚举中定义了所有的事件类型：CHILD_ADDED CHILD_UPDATE CHILD_REMOVED
            构造参数说明
            dataIsCompressed 是否进行数据压缩
            cacheData 是否把节点内容缓存起来，如果配置为true，客户端收到节点列表变更的同时，也能够获取到节点的数据内容；如果配置为false，则无法获取到节点的数据内容；
            threadFactory + executorService 自定义线程池来处理事件通知
             */
            PathChildrenCache cache = new PathChildrenCache(client, path, true);
            cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
            cache.getListenable()
                    .addListener(new PathChildrenCacheListener() {
                        @Override
                        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                            switch (event.getType()){
                                case CHILD_ADDED:
                                    logger.info("child_added: {}", event.getData().getPath());
                                    break;
                                case CHILD_UPDATED:
                                    logger.info("child_updated: {}", event.getData().getPath());
                                    break;
                                case CHILD_REMOVED:
                                    logger.info("child_removed: {}", event.getData().getPath());
                                    break;
                                default:
                                    break;
                            }
                        }
                    });
            client.create().withMode(CreateMode.PERSISTENT).forPath(path);
            Thread.sleep(1000);
            client.create().withMode(CreateMode.PERSISTENT).forPath(path+"/c9");
            client.delete().forPath(path+"/c9");
            client.delete().forPath(path);//节点本身的变更不会通知到PathChildrenCacheListener
            //curator也无法监听孙子节点的变更（节点 path+"/c9/c10" 的删除）
        }
        /*
        Master选举
        场景举例：对于一个复杂任务，仅需要从集群中选举出一台进行处理，这台机子可视为master
        实现思路：选择一个根节点，如/master_select 多台机器同时像该节点创建一个子节点，最终只有一台机器能够成功，这台机子就是master
        新建的子节点格式：_c_15d347tg-ger8-dnguf-8t8789thb9-lock-0000000023 最后的数字是递增的且不断增加
         */
        static void masterSelect() throws Exception{
            String master_path="/master_path";
            client.start();
            LeaderSelector selector = new LeaderSelector(client, master_path,
                    new LeaderSelectorListenerAdapter() {
                        @Override
                        public void takeLeadership(CuratorFramework client) throws Exception {
                            logger.info("I took leadership among cluster");
                            Thread.sleep(3000);
                            logger.info("job finished, releasing leadership");
                        }
                    });
            selector.autoRequeue();
            selector.start();
            Thread.sleep(4000);
        }
        /*
        分布式锁 - 并发场景下保证唯一递增,如订单编号的生成
         */
        static void curatorDistributedLock(){
            String lock_path = "/lock";
            client.start();
            InterProcessMutex mutext = new InterProcessMutex(client, lock_path);
            CountDownLatch countDownLatch = new CountDownLatch(1);
            for (int i = 0; i < 16; i++) { //core i9
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            countDownLatch.await();
                            mutext.acquire();//获取分布式锁
                        }catch (Exception e){e.printStackTrace();}
                        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");
                        String orderNo = format.format(new Date());
                        logger.info("thread:{} generate orderNo = {}", Thread.currentThread().getName(), orderNo);
                        try { //释放分布式锁
                            mutext.release();
                        } catch (Exception e) {throw new RuntimeException(e);}
                    }
                }).start();
            }
            //主线程控制所有子线程同时开始竞争分布式锁
            countDownLatch.countDown();
        }
        /*
        分布式计数器 DistributedAtomicInteger
        应用场景：统计系统的在线人数
        实现思路：指定一个ZooKeeper数据节点作为计数器，多个应用实例在分布式锁的控制下，通过更新该数据节点的内容实现计数功能
         */
        static void distributedCounter() throws Exception{
            String path = "/atomic_counter";
            client.start();
            DistributedAtomicInteger atomicInteger =
                    new DistributedAtomicInteger(client, path, new RetryNTimes(3, 1000));
            AtomicValue<Integer> add = atomicInteger.add(8);
            logger.info("{}->{}, success = {}", add.preValue(), add.postValue(), add.succeeded());
        }
        /*
        分布式Barrier
        CyclicBarrier是控制多线程之间同步的经典方式,可以很好地在同一个JVM中控制所有线程处于就绪状态后才执行后续操作
        分布式环境中，Curator提供了 DistributedBarrier
         */
        static DistributedBarrier barrier;
        static void cyclicBarrier() throws Exception{
            String path = "/barrier";
            for (int i = 0; i < 5; i++) {
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        client.start();
                        barrier = new DistributedBarrier(client, path);
                        logger.info("{} start to set barrier", Thread.currentThread().getName());
                        try {
                            barrier.setBarrier();
                            barrier.waitOnBarrier();
                        }catch (Exception e){e.printStackTrace();}
                        logger.info("starting");
                    }
                }).start();
            }
            Thread.sleep(3000);
            barrier.removeBarrier();//触发所有线程结束Barrier等待，开始执行业务逻辑
        }
        /*
        上面的Barrier是由主线程触发，Curator还提供了DistributedDoubleBarrier
         */
        static void ditributedDoubleBarrier(){
            String path = "/barrier";
            client.start();
            for (int i = 0; i < 5; i++) {
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        //??是否可以移到外面
                        DistributedDoubleBarrier barrier =
                                new DistributedDoubleBarrier(client, path, 5);//5
                        try {
                            Thread.sleep(Math.round(Math.random() * 3000));
                            logger.info("thread: {} enter barrier", Thread.currentThread().getName());
                            barrier.enter();//每个Barrier参与者在enter后会进行等待，当进入的成员数量到达5时，所有的成员会被同时触发进入
                            logger.info("thread: {} working", Thread.currentThread().getName());
                            barrier.leave();//当执行leave的成员数量达到5个时，所有成员可以同时执行后续的逻辑
                            logger.info("thread: {} leaved", Thread.currentThread().getName());
                        } catch (Exception e) {throw new RuntimeException(e);}
                    }
                }).start();
            }
        }
        /*
        Curator 工具类：ZKPaths EnsurePath
         */
        static void howtoZKPaths() throws Exception{
            String nameSpace = "/toolset";
            String path = "/c5";
            client.start();
            ZooKeeper zooKeeper = client.getZookeeperClient().getZooKeeper();
            String fixNameSpace = ZKPaths.fixForNamespace(nameSpace, path);
            logger.info("fixNameSpace = {}", fixNameSpace);//调整path到某个namespace下
            String mkPathResult = ZKPaths.makePath(nameSpace, path);
            logger.info("mkPathResult={}", mkPathResult);
            String nodeFromPath = ZKPaths.getNodeFromPath(nameSpace + path);
            logger.info("nodeFromPath={}", nodeFromPath);// c5
            ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(nameSpace + path);
            logger.info("getPathAndNode:{},{}", pathAndNode.getNode(), pathAndNode.getPath());//c5,/toolset
            String dir = nameSpace+path+"/cc1";
            ZKPaths.mkdirs(zooKeeper, dir);
            List<String> sortedChildren = ZKPaths.getSortedChildren(zooKeeper, nameSpace);
            logger.info("sortedChildren={}",sortedChildren);//[c5]  不会把孙子节点也查出来
            //ZKPaths.deleteChildren(zooKeeper, nameSpace, true);
        }
        /*
        EnsurePath提供了一种能够确保数据节点存在的机制
        应用场景：上层业务希望对一些数据节点进行操作，但在操作之前需要确保该节点存在，同时创建节点时还要考虑并发问题，如果使用原生API将会非常麻烦
        EnsurePath采取了静默的节点创建方式，内部实现就是试图创建指定节点，如果节点已存在，就不进行任何操作，也不对外抛出异常，否则正常创建数据节点
        该类已弃用（2.9），推荐使用
        Prefer CuratorFramework.create().creatingParentContainersIfNeeded() or CuratorFramework.exists().creatingParentContainersIfNeeded()
         */
        static void howtoEnsurePath(){
            String namespace = "toolset";
            String path = "/c5";
            client.start();
            client.usingNamespace(namespace);
            new EnsurePath(path);
        }
        //不安装ZooKeeper也可以开发调试Curator，需要引入 curator-test 包，使用其中的 TestingServer TestingCluster(集群) 类
        public static void main(String[] args) throws Exception {
            //curatorDistributedLock();
            //distributedCounter();
            howtoZKPaths();
        }

    }

    public static void main(String[] args) {
        System.out.println("Hello world!");
    }
}