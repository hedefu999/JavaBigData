package com.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Hbase2API {
    public static final String TABLENAME = "tableName";
    public static final String ROWKEY = "rowKey";
    public static final String FAMILY = "family";
    public static final String QUALIFIER = "qualifier";
    public static final byte[] TABLENAME_BYTES = Bytes.toBytes(TABLENAME);
    public static final byte[] ROWKEY_BYTES = Bytes.toBytes(ROWKEY);
    public static final byte[] FAMILY_BYTES = Bytes.toBytes(FAMILY);
    public static final byte[] QUALIFIER_BYTES = Bytes.toBytes(QUALIFIER);

    private static Connection DEFAULT_CONNECTION = getConnection(null);

    static Connection getConnection(ExecutorService pool){
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "10.199.171.164");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("zookeeper.znode.parent", "/hbase");
        try {
            if (pool == null){
                return ConnectionFactory.createConnection(config);
            }
            return ConnectionFactory.createConnection(config, pool);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    //静态变量初始化方法声明异常怎么写？构造函数声明下这个异常
    //但如果这个field是static的，就只能try catch了
    private Table hTable = getHTable(false);
    public Hbase2API() throws Exception {
    }


    /*
     拿到Table实现类可以进行put delete incr等操作
     */
    static Table getHTable(boolean useConnectionPool) throws Exception{
        Table table;
        if (useConnectionPool){
            ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(2, 4, 600,
                    TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
            poolExecutor.prestartAllCoreThreads();
            table = DEFAULT_CONNECTION.getTableBuilder(TableName.valueOf(TABLENAME), poolExecutor).build();
        } else {
            table = DEFAULT_CONNECTION.getTable(TableName.valueOf(TABLENAME));
        }
        return table;
    }

    /**-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=*/
    static boolean tableExist(String tableName){
        try (Admin admin = DEFAULT_CONNECTION.getAdmin()) {
            return admin.tableExists(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /*
    新版API中使用
    TableDescriptorBuilder 表描述生成器、ColumnFamilyDescriptorBuilder 列簇描述生成器
    替代原来的 HTableDescriptor和HColumnDescriptor
     */
    static void createTable(String tableName, String... families) throws Exception{
        TableName tName = TableName.valueOf(tableName);
        TableDescriptorBuilder tableDescBuilder = TableDescriptorBuilder.newBuilder(tName);
        for (String family : families){
            ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.of(family);
            tableDescBuilder.setColumnFamily(familyDescriptor);
        }
        TableDescriptor tableDescriptor = tableDescBuilder.build();
        try(Admin admin = DEFAULT_CONNECTION.getAdmin()){
            admin.createTable(tableDescriptor);
        }
    }

    /*
    已创建的表增加family
     */
    static void addColumnFamily(String tableName, String... columnFamily) throws Exception{
        try (Admin admin = DEFAULT_CONNECTION.getAdmin()){
            for (String family : columnFamily){
                ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.of(family);
                admin.addColumnFamily(TableName.valueOf(tableName), familyDescriptor);
            }
        }
    }

    /*
    删除表
     */
    static void deleteTable(String tableName) throws Exception{
        TableName tName = TableName.valueOf(tableName);
        try (Admin admin = DEFAULT_CONNECTION.getAdmin()) {
            //先禁用表再删除表
            admin.disableTable(tName);
            admin.deleteTable(tName);
        }
    }
    /**-=-=-=-=-=-=--=-=-=-=- Get API -=-=-=-=-=-=-=-=-=-=-=-=-=*/
    static void get(String tableName, String rowKey, String family, String qualifier) throws Exception{
        TableName tName = TableName.valueOf(tableName);
        try (Table table = DEFAULT_CONNECTION.getTable(tName)){
            Get get = new Get(Bytes.toBytes(rowKey));
            //get.addFamily(Bytes.toBytes(family)); 如果查询指定列簇
            //get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier)); 查询指定列簇的列
            Result result = table.get(get); //一个 result 包含一个rowKey的数据
            byte[] valueBytes = result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            String value = Bytes.toString(valueBytes);
            System.out.println(value);

            //有result就可以得到Cell
            Cell[] cells = result.rawCells();
            for (Cell cell : cells){
                Bytes.toString(CellUtil.copyRow(cell));//rowKey
                Bytes.toString(CellUtil.cloneFamily(cell));//family name
                Bytes.toString(CellUtil.cloneQualifier(cell));//qualifier name
                Bytes.toString(CellUtil.cloneValue(cell));//qualifier value
                cell.getTimestamp();//row的时间戳
            }
        }
    }

    static void batchGet(String tableName, String... rowKeys) throws Exception{
        TableName tName = TableName.valueOf(tableName);
        try (Table table = DEFAULT_CONNECTION.getTable(tName)){
            List<Get> gets = new ArrayList<>();
            for (String rowKey : rowKeys){
                Get get = new Get(Bytes.toBytes(rowKey));
                gets.add(get);
            }
            Result[] results = table.get(gets);
            for (Result result : results){
                //... 见单条get的写法
            }
        }
    }

    /*
    使用Scan查询数据
     */
    static void useScan(String tableName, String family, String column) throws Exception{
        try (Table table = DEFAULT_CONNECTION.getTable(TableName.valueOf(tableName))){
            Scan scan = new Scan();
            //下述 三选一
            scan.setRowPrefixFilter(Bytes.toBytes("rowPrefixXXX"));//根据前缀筛选
            scan.withStartRow(Bytes.toBytes("AAA001")).withStopRow(Bytes.toBytes("AAA005"));//按rowKey范围进行行扫描
            scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));//

            ResultScanner scanner = table.getScanner(scan);
            Result result;
            while ((result = scanner.next()) != null) {
                CellScanner cellScanner = result.cellScanner();
                while (cellScanner.advance()){
                    Cell current = cellScanner.current();
                    System.out.println("rowkey = "+ Bytes.toString(CellUtil.copyRow(current)));
                    System.out.println("family = "+ Bytes.toString(CellUtil.cloneFamily(current)));
                }
            }
        }
    }


    //对Scan API进行压测
    static byte[] TAG_RAW_FAMILY = Bytes.toBytes("cf");
    static byte[] TAG_RAW_QUALIFIER = Bytes.toBytes("v");
    static TableName TAGRAW_TABLENAME = TableName.valueOf("tag_raw");
    static ExecutorService badPool = new ThreadPoolExecutor(1,1,100, TimeUnit.SECONDS,new LinkedBlockingDeque<>(1), Executors.defaultThreadFactory());
    //直接Discard也会抛出异常：超时异常
    static ExecutorService badNoExceptionPool = new ThreadPoolExecutor(1,1,100, TimeUnit.SECONDS,new LinkedBlockingDeque<>(1),
            Executors.defaultThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());

    static String[] array = {"a","b","c","d","e"};
    static void insertIntoTagRaw(String rowKeyPrefix) {
        try (Table table = DEFAULT_CONNECTION.getTable(TAGRAW_TABLENAME)) {
            long start=197001010800L;
            for (int i = 0; i < 5; i++) {
                String index = start + array[i];
                String rowKey = rowKeyPrefix + ":" + index;
                System.out.println(rowKey);
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(TAG_RAW_FAMILY, TAG_RAW_QUALIFIER,Bytes.toBytes("value"+array[i]));
                table.put(put);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        /*
        b:idno_userid:52oQbB3rX1moBYLxqH2mp+IH2Ip3AEKb:1970010108001-10
        b:idno_userid:52oQbB3rX1moBYLxqH2mp+IH2Ip3AEKb:a-e
        b:idno_userid:52oQbB3rX1moBYLxqH2mp+IH2Ip3AEKb:197001010800a-e
         */
    }

    public static void main(String[] args) {
        //insertIntoTagRaw("b:idno_userid:52oQbB3rX1moBYLxqH2mp+IH2Ip3AEKb");
        batchScan();
    }
    static void batchScan(){
        Connection defaultBatchPoolConn = getConnection(badNoExceptionPool);

        try (Table tag_raw = defaultBatchPoolConn.getTable(TAGRAW_TABLENAME)){
            Callable<String> callable = new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return scanAtomicOperation(tag_raw);
                }
            };
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    try {
                        scanAtomicOperation(tag_raw);
                    } catch (Exception e) {
                        System.out.println("这就是线上报错：" + e.getCause());
                    }
                }
            };
            for (int i = 0; i < 10; i++) {
                FutureTask<String> futureTask = new FutureTask<>(callable);
                new Thread(futureTask).start();
                try {
                    String result = futureTask.get();//10, TimeUnit.MILLISECONDS);
                    System.out.println("获得结果："+ result);
                }catch (Exception e){
                    System.out.println("future task get时发生错误："+e.getCause());
                }
            }
        }catch (Exception e){
            System.out.println("这就是外层报错：" + e.getCause());
        }
    }

    static String scanAtomicOperation(Table table) throws Exception{
        List<String> collector = new ArrayList<>();
        String currentThreadName = Thread.currentThread().getName();
        String rowPrefix = "b:idno_userid:52oQbB3rX1moBYLxqH2mp+IH2Ip3AEKb";
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(rowPrefix + ":1970010108000"));
        scan.withStopRow(Bytes.toBytes(rowPrefix + ":aaaaaaaaaaaa"));
        scan.addColumn(TAG_RAW_FAMILY, TAG_RAW_QUALIFIER);
        scan.readVersions(1);
        int caching = scan.getCaching();
        if (caching == 1){
            System.out.println("change caching");
            scan.setCaching(500);
        }
        try (ResultScanner scanner = table.getScanner(scan)) {
            //System.out.println(currentThreadName + "start to sleep");
            //TimeUnit.SECONDS.sleep(5);
            for (Result result : scanner) {
                Cell cell = result.getColumnLatestCell(TAG_RAW_FAMILY, TAG_RAW_QUALIFIER);
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                collector.add(value);
            }
        }
        String result = currentThreadName + " finished with " + collector.size() + " results";
        //System.out.println(result);
        return result;
    }

    /**-=-=-=-=-=-=--=-=-=-=-=- Put API =-=-=-=-=-=-=-=-=-=-=-=-=*/
    static void putSingleLine() throws Exception{
        Put put = new Put(Bytes.toBytes("rowKey"));
        put.addColumn(FAMILY_BYTES, QUALIFIER_BYTES, Bytes.toBytes("value"));
        Table hTable = getHTable(false);
        hTable.put(put);
    }

    static void batchPut() throws Exception{
        List<Put> puts = new ArrayList<>();
        puts.add(new Put(ROWKEY_BYTES).addColumn(FAMILY_BYTES, QUALIFIER_BYTES, Bytes.toBytes("value")));
        //...
        getHTable(false).put(puts);
    }

    /**-=-=-=-=-=-=--=-=-=-=-=- Delete API -=-=-=-=-=-=-=-=-=-=-=-=*/
    static void useDeleteAPI(String tableName, String family, String qualifier, String... rowkeys) throws Exception{
        try (Table table = DEFAULT_CONNECTION.getTable(TableName.valueOf(tableName))){
            List<Delete> deletes = new ArrayList<>();
            for (String rowkey : rowkeys){
                Delete delete = new Delete(Bytes.toBytes(rowkey));
                delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));//删除rowkey下指定的列簇下指定的列
                delete.addFamily(Bytes.toBytes(family));//删除rowkey下的整个列簇
                deletes.add(delete);
            }
            table.delete(deletes);
        }
    }

    /**-=-=-=-=-=-=-=-=-=-=-=-=-= Increment: Hbase计数器  -=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
    /*
     同一个rowkey下有多个qualifier时分别进行incr
     */
    static void batchIncrementSameRowKey() throws Exception{
        Table hTable = getHTable(false);
        Increment increment = new Increment(Bytes.toBytes("rowKey"));
        increment.addColumn(FAMILY_BYTES, QUALIFIER_BYTES, 1L);
        increment.addColumn(Bytes.toBytes(FAMILY + "01"), Bytes.toBytes(QUALIFIER + "01"), 1L);
        //...
        hTable.increment(increment);
    }
    /*
    不同的rowKey多条记录要批量incr, 使用batch api， batch api 还支持 Put Get ....
     */
    static void batchIncrementDiffRowKey() throws Exception{
        Table hTable = getHTable(false);
        List<Increment> increments = new ArrayList<>(2);
        Increment increment = new Increment(Bytes.toBytes("rowKey"));
        increment.addColumn(FAMILY_BYTES, QUALIFIER_BYTES, 1L);
        Increment increment2 = new Increment(Bytes.toBytes("rowKey2"));
        increment2.addColumn(Bytes.toBytes("family2"), Bytes.toBytes("qualifier2"), 1L);
        increments.add(increment);
        increments.add(increment2);
        //results数组的长度必须与increments中实际的元素数量一致，否则报错
        Object[] results = new Object[increments.size()];
        hTable.batch(increments, results);
        System.out.println(Arrays.toString(results));
        /*
        [keyvalues={rowkey01/cf:count/1652949042865/Put/vlen=8/seqid=0}, keyvalues={rowkey02/cf:count/1652949042867/Put/vlen=8/seqid=0}, keyvalues={rowkey03/cf:count/1652949042868/Put/vlen=8/seqid=0}]
         */
    }

}
