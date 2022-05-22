package com.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTableTest {
    //private static final Logger logger = LoggerFactory.getLogger("this.getClass().getSimpleName()");

    public static final String tableName = "fraud_rtc_user_app_page_statistics";
    public static final String commonRowKey = "3277543_20220118";
    public static final String family = "ascore_vars";
    public static final String qualifier = "page_on_time_sum";
    public static final String hbase_cluster_staging = "10.199.174.151";
    public static final String hbase_cluster_mine = "10.107.77.226";
    public static final String host_name = "VIP-20211024LSQ.local";
    /**
     hbase.zookeeper.quorum: hbase
     hbase.zookeeper.property.clientPort: 2181
     hbase.zookeeper.parent: /hbase
     hbase.rootdir: hdfs://namenode:9000/hbase
     */
    public static void tryNewVersionHbase() throws Exception{
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum",hbase_cluster_mine);
        //config.set("hbase.zookeeper.property.clientPort","2181");
        //config.set("hbase.zookeeper.parent","/hbase");
        Connection connection = ConnectionFactory.createConnection(config);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setCaching(5000);
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            byte[] row = result.getRow();
            byte[] value = result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            System.out.println(Bytes.toLong(value));
        }
    }

    public static void main(String[] args) throws Exception {
        tryNewVersionHbase();
    }
}
