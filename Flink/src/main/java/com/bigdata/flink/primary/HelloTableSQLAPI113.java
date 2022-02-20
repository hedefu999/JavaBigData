package com.bigdata.flink.primary;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;

/*
依赖 flink-table_2.11 1.7.0+ 的Table API写法
flink版本1.7.+ 《Flink原理、实战与性能优化》 - 张利兵
不推荐

luweizheng 《Flink原理与实践》 使用版本 1.11.0

本项目使用 1.13.5 采用 Flink原理与实践 中的写法
 */
public class HelloTableSQLAPI113 {
    static StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    static ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
    //static final StreamTableEnvironment streamTableEnv = TableEnvironment.getTableEnvironment(streamEnv);
    //static final BatchTableEnvironment tableBatchEnv = TableEnvironment.getTableEnvironment(batchEnv);

}
