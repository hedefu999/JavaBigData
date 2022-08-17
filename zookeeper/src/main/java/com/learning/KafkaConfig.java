package com.learning;

public class KafkaConfig {
    //集群环境下，理论上只要一个broker地址就能找到其他broker，但如果这个broker恰好发生宕机就无法连接了。所以至少要配置两个以上，将集群的所有broker配置上更好
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String GROUP_ID = "group.demo";
    public static final String TOPIC_TEST = "test_topic";
}
