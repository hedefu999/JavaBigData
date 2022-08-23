package com.learning;

import kafka.admin.TopicCommand;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static com.learning.KafkaConfig.BOOTSTRAP_SERVERS;
import static com.learning.KafkaConfig.GROUP_ID;
import static com.learning.KafkaConfig.TOPIC_TEST;

/**
 * 使用教材《深入理解Kafka_核心设计与实践原理》
 */
public class KafkaConsumerPrimary {
    private static final CountDownLatch COUNT_DOWN_LATCH = new CountDownLatch(1);

    private static final KafkaConsumer<Integer, String> consumer;
    static {
        Properties properties = new Properties();
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        //设置消费组名称
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        //这个client.id不设置的话，kafka会分配一系列 consumer-1 consumer-2 样式的字符串
        //properties.put("client.id", "consumer.client.id.demo");
        addCustomerInterceptor(properties);
        consumer = new KafkaConsumer<Integer, String>(properties);
    }
    static void nonAutoCommit(Properties properties){
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString().toLowerCase());
    }
    static void addCustomerInterceptor(Properties properties){
        //KafkaConsumer 拦截器
        properties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, AboutConsumerInterceptor.CustomerInterceptorTTL.class.getName());
    }

    static class CustomizeSerializer{
        @Data @NoArgsConstructor @AllArgsConstructor
        static class Company{
            private String name;
            private String address;
        }
        static class CompanySerializer implements Serializer<Company>{
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public byte[] serialize(String topic, Company data) {
                if (data == null){
                    return null;
                }
                byte[] name, address;
                try {
                    name = Optional.ofNullable(data.getName()).map(String::getBytes).orElse(new byte[]{});
                    address = Optional.ofNullable(data.getAddress()).map(String::getBytes).orElse(new byte[]{});
                    ByteBuffer buffer = ByteBuffer.allocate(4+4+name.length+address.length);
                    buffer.putInt(name.length);
                    buffer.put(name);
                    buffer.putInt(address.length);
                    buffer.put(address);
                    return buffer.array();
                }catch (Exception e){}
                return new byte[0];
            }
            @Override
            public void close() {
            }
        }
    }
    /**
     * 自定义一个ProducerInterceptor
     * 为消息增加前缀 prefix1-
     */
    static class CustomizeInterceptor{
        static class ProducerInterceptorPrefix implements ProducerInterceptor<String,String>{
            private volatile long sendSuccess = 0;
            private volatile long sendFail = 0;
            @Override
            public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
                String modifiedValue = "prefix1-" + record.value();
                ProducerRecord<String, String> modifiedRecord = new ProducerRecord<>(record.topic(),
                        record.partition(), record.timestamp(), record.key(), modifiedValue, record.headers());
                return modifiedRecord;
            }
            @Override
            public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
                if (exception == null) sendSuccess ++;
                else sendFail ++;
            }
            @Override
            public void close() {
                double ratio = (double) sendSuccess / (sendSuccess + sendFail);
                System.out.printf("[INFO] 当前发送成功率 %f %% %n", ratio * 100);
            }
            @Override
            public void configure(Map<String, ?> configs) {
            }
        }
    }

    static class Consumer{
        private static final AtomicBoolean isRunning = new AtomicBoolean(true);
        public Consumer() {}
        public String consume(){
            consumer.subscribe(Collections.singletonList(TOPIC_TEST));
            //poll方法中传递的超时时间参数控制poll方法的阻塞时间，在消费者缓冲区里没有可用数据时会发生阻塞
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<Integer,String> record : records){
                System.out.println(record.value());
            }
            return "";
        }
        public void subscribeTopic(String topic){
            /**
             Consumer可以subscribe多个topic，作为集合传入，但多次调用subscribe方法只会后来者覆盖
             如果使用了正则表达式进行订阅，新创建的匹配正则的topic会被自动加进来
             */
            consumer.subscribe(Arrays.asList(topic));
            consumer.subscribe(Pattern.compile("aaa-*") /*,负载均衡监听器*/);
            try { //ConsumerRecords API介绍
                while (isRunning.get()){
                    //ConsumerRecords演示了如果实现一个Iterable
                    ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(1000));
                    //0. 消息数量
                    int count = records.count();
                    //是否有消息
                    boolean empty = records.isEmpty();
                    // 主动 获取一个空的消息集，这是一个static常量
                    ConsumerRecords<Object, Object> empty1 = ConsumerRecords.empty();
                    //1. 可以对ConsumerRecords进行foreach操作
                    for (ConsumerRecord<Integer,String> record : records){
                        String topic1 = record.topic();
                        int partition = record.partition();
                        long offset = record.offset();
                        long timestamp = record.timestamp();
                        System.out.println(record.key() + record.value());
                    }
                    //2. 获取指定分区的消息
                    List<ConsumerRecord<Integer, String>> specifiedPartitionRecords =
                            records.records(new TopicPartition(topic, 2));
                    //3. 处理消息时获知是哪个分区的
                    Set<TopicPartition> partitions = records.partitions();
                    for (final TopicPartition partition : partitions) {
                        for (final ConsumerRecord<Integer, String> record : records.records(partition)) {
                            int partition1 = record.partition();
                            String value = record.value();
                            System.out.printf("%d分区的消息内容：%s%n", partition1, value);
                        }
                    }

                }
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                consumer.close();
            }
        }

        /**
         Kafka Consumer 支持直接订阅主题分区 TopicPartition
         */
        public void subscribeTopicPartition(String topic){
            //consumer提供了方法进行分区查询
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            //希望consumer订阅所有分区
            List<TopicPartition> topicAllPartitions = new ArrayList<>();
            if (partitionInfos != null){
                for (PartitionInfo info : partitionInfos){
                    topicAllPartitions.add(new TopicPartition(info.topic(), info.partition()));
                }
            }
            consumer.assign(topicAllPartitions);

            //取消订阅
            consumer.unsubscribe();
            //订阅空集合也是取消订阅
            consumer.subscribe(Collections.emptyList());
            consumer.assign(Collections.emptyList());
        }
    }

    static class CustomizeDeserializer{
        static class CompanyDeserializer implements Deserializer<CustomizeSerializer.Company>{
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {}
            @Override
            public CustomizeSerializer.Company deserialize(String topic, byte[] data) {
                if (data == null || data.length == 0){
                    return null;
                }
                ByteBuffer byteBuffer = ByteBuffer.wrap(data);
                String name = null, address = null;

                byte[] nameBytes = new byte[byteBuffer.getInt()];
                byteBuffer.get(nameBytes);

                byte[] addressBytes = new byte[byteBuffer.getInt()];
                byteBuffer.get(addressBytes);
                try {
                    name = new String(nameBytes);
                    address = new String(addressBytes);
                }catch (Exception e){
                    e.printStackTrace();
                }
                return new CustomizeSerializer.Company(name, address);
            }
            @Override
            public void close() {}
        }
    }
    /**
     * 关于Kafka中的offset概念
     */
    //private static final ScheduledExecutorService SCHEDULED_EXECUTOR = Executors.newSingleThreadScheduledExecutor();
    static class AboutOffset{
        static void showOffsetFeatures(String topic, int partition){
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            consumer.assign(Arrays.asList(topicPartition));
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            System.out.printf("partitions: %s", partitionInfos.toString());
            long lastConsumedOffset = -1;//当前消费到的offset
            ConsumerRecords<Integer, String> records = consumer.poll(Long.MAX_VALUE);
            if (records.isEmpty()){
                System.out.printf("that's impossible");
                return;
            }
            List<ConsumerRecord<Integer, String>> partitionRecords = records.records(topicPartition);
            lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
            //同步提交消费位移（手动提交，需要将 enable.auto.commit 设置为 false）
            //consumer.commitAsync(); consumer.commitSync();
            //当分区首次消费时 consumed offset 从0开始打印，表示已消费的消息的位置
            System.out.printf("consumed offset = %d\n", lastConsumedOffset);
            OffsetAndMetadata offsetAndMetadata = consumer.committed(topicPartition);
            //committed后消息的 offset 从1开始计数，且 offsetAndMetadata.offset() = lastConsumerRecord.offset() + 1
            System.out.printf("offset = %d, metadata = %s\n", offsetAndMetadata.offset(), offsetAndMetadata.metadata());
            //此行打印offset与上面的 offsetAndMetadata.offset() 相同
            System.out.printf("the offset of next record is %d\n", consumer.position(topicPartition));
        }

        /**
         * offset 手动提交需要注意啥
         *  - 同步提交 commitSync
         *  - 异步提交 commitAsync
         */
        private static final AtomicBoolean PROG_RUNNING = new AtomicBoolean(true);
        private static List<ConsumerRecord<Integer,String>> buffer = new ArrayList<>();
        static void commitOffsetSync(){
            while (PROG_RUNNING.get()){
                ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(1));
                if (records.count() > 200){
                    consumer.commitSync();//前置offset commit 易丢失消息
                }
                /*-=-=-=-=-=-=-=-=-=-=-*/
                for (final ConsumerRecord<Integer, String> record : records) {
                    buffer.add(record);
                }
                for (final ConsumerRecord<Integer, String> record : buffer) {
                    //do some business logical
                }
                //后置offset提交，会因上面的业务处理异常产生重复消费
                if (buffer.size() > 200){
                    consumer.commitSync();
                    buffer.clear();
                }
            }
        }
        /**
         * 更细粒度地提交offset，按分区指定offset进行位移提交，实现每消费一条消息就进行一次提交
         * 实际应用中很少会这样每消费一条消息就提交一次offset，commitSync 同步方法本身消耗性能
         */
        static void commitOffsetSyncFineGrained(){
            while (PROG_RUNNING.get()){
                ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(1));
                for (final ConsumerRecord<Integer, String> record : records) {
                    //do some business logical

                    /** 注意 record.offset 是当前消费的消息的位移量（相当于消息的位移主键ID），而进行消费位移提交时使用的offset应该是下一次poll消息的起始offset，所以需要 加1 */
                    long offset = record.offset();
                    OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset + 1);
                    TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                    consumer.commitSync(Collections.singletonMap(topicPartition, offsetAndMetadata));
                }
            }
        }
        /**
         * 上面一条消息提交一次的操作效率太低，可以改成同一批次消息每个分区提交一次
         */
        static void commitOffsetSyncByPartition(){
            try {
                while (PROG_RUNNING.get()){
                    ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(1));
                    for (final TopicPartition partition : records.partitions()) {
                        List<ConsumerRecord<Integer, String>> partitionRecords = records.records(partition);
                        for (final ConsumerRecord<Integer, String> partitionRecord : partitionRecords) {
                            //do some business logical
                        }
                        //获取最后一条消息的offset
                        long lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(lastConsumedOffset + 1);
                        consumer.commitSync(Collections.singletonMap(partition, offsetAndMetadata));
                    }
                }
            }finally {
                consumer.close();
            }
        }

        /**
         异步提交通过 OffsetCommitCallback 这个异步回调接收提交结果
         */
        static void commitOffsetAsyncWithCallback(){
            while (PROG_RUNNING.get()){
                ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(1));
                for (final ConsumerRecord<Integer, String> record : records) {
                    //do some business logical
                }
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if (exception == null){
                            System.out.println(offsets);
                        }else {
                            System.out.printf("fail to commit offset %s %s\n",offsets.toString(), exception.getCause());
                        }
                    }
                });
            }
        }
        /**
         * commitAsync 失败后，可以进行重试
         */
        static void commitOffsetAsync(){
            try {
                //消费消息
                consumer.commitAsync();
            }finally {
                try {
                    consumer.commitAsync();
                }finally {
                    consumer.close();
                }
            }
        }
        static void bestPracticeInMessageConsuming(){
            //一个相对完整的消费流程
            consumer.subscribe(Arrays.asList(TOPIC_TEST));
            //pause() 方法可以暂停某些分区的消息的消费
            List<TopicPartition> topicPartitions = Arrays.asList(new TopicPartition(TOPIC_TEST, 0));
            consumer.pause(topicPartitions);
            consumer.resume(topicPartitions);
            //通过 paused() 方法获取暂停消费的分区
            Set<TopicPartition> paused = consumer.paused();
            try {
                while (PROG_RUNNING.get()){

                }
            // consumer.wakeup() 方法执行后会发生WakeupException异常，可以借此跳出消费循环，且捕获时不必处理这个异常。wakeup是KafkaConsumer中唯一一个线程安全的方法
            }catch (WakeupException e){
                //ignore the error
            } catch (Exception e){
                //
            } finally {
                //commit offset manually
                //close 方法可以传递超时时间，表示等待close方法中的资源释放（内存资源、Socket连接等）的最长时间，不传递默认等待30s（见源码）
                consumer.close();
            }
            //上述这个消费逻辑的关闭可以选择两种方式： consumer.wakeup(); 或者 PROG_RUNNING.set(false)
        }
        static void abountOffsetResetStrategy(){
            consumer.subscribe(Arrays.asList(TOPIC_TEST));
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(1));
            System.out.printf("几个？ %d\n", records.count());
            //consumer.commitAsync();

            /**-=-=-=-=-=-=-= 更细粒度地控制Consumer的消费位移 -=-=-=-=-=-*/
            // 通过assignment()方法得到当前消费者订阅的分区集合
            Set<TopicPartition> topicPartitions = consumer.assignment();
            System.out.printf("得到 %d 个分区的消息\n", topicPartitions.size());
            for (final TopicPartition topicPartition : topicPartitions) {
                //从上面的分区集合里选择一个，并指定offset，通过seek()方法可以更精确地控制KafkaConsumer的消费位移
                consumer.seek(topicPartition, 1);
            }
            ConsumerRecords<Integer, String> records2 = consumer.poll(Duration.ofSeconds(1));
            System.out.printf("这次拉到%d条消息", records2.count());
        }
        /**
         * 上述方法存在一个漏洞，如果poll(Duration.ofSeconds(0)),此时assignment就得不到任何分区
         * 代码应该这样写
         */
        static void assignAfterPartitionDistributed(){
            consumer.subscribe(Arrays.asList(TOPIC_TEST));
            //如果对未分配到的分区执行 seek() 方法，会报 IllegalStateException: No current assignment for partition test_topic-0
            consumer.seek(new TopicPartition(TOPIC_TEST, 0), 1);
            //采用轮询方式等待消息分区分配，间隔100ms
            Set<TopicPartition> assignment = new HashSet<>();
            while (assignment.isEmpty()){
                consumer.poll(Duration.ofMillis(100));
                System.out.println("运行了一次"); //本地运行打印了两次
                assignment = consumer.assignment();
            }
            for (final TopicPartition topicPartition : assignment) {
                consumer.seek(topicPartition, 1);
            }
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(1000));
            System.out.printf("拉取到%d条消息\n", records.count());

            /**-=-=-=-=-= 消费位移控制的其他方法 -=-=-=-=-=*/
            Map<TopicPartition, Long> topicPartitionAndLastOffset = consumer.endOffsets(assignment);
            for (final TopicPartition topicPartition : assignment) {
                consumer.seek(topicPartition, topicPartitionAndLastOffset.get(topicPartition));
            }

            /*-=-=-=-= Kafka提供了专门的方法从开始和结尾处进行消息消费 =-=-=-=-*/
            consumer.seekToBeginning(assignment);
            consumer.seekToEnd(assignment);
        }
        /*
        寻找某个时间戳之后的消息的最小offset
        如寻找近1小时内的全部消息
         */
        static void seekMessageOffsetByTimestamp(){
            Map<TopicPartition, Long> timestamp2Search = new HashMap<>();
            Set<TopicPartition> assignment = consumer.assignment();
            for (final TopicPartition topicPartition : assignment) {
                timestamp2Search.put(topicPartition, System.currentTimeMillis() - 3600*1000);
            }
            Map<TopicPartition, OffsetAndTimestamp> specifiedTimestampOffsetInfo = consumer.offsetsForTimes(timestamp2Search);
            for (final TopicPartition topicPartition : assignment) {
                OffsetAndTimestamp offsetAndTimestamp = specifiedTimestampOffsetInfo.get(topicPartition);
                System.out.printf("we know latest message offset one hour before of this partition %d  is %d, and its timestamp is %d\n", topicPartition.partition(), offsetAndTimestamp.offset(), offsetAndTimestamp.timestamp());
                consumer.seek(topicPartition, offsetAndTimestamp.offset());
            }
            //从指定位置开始消费
            consumer.poll(Duration.ofSeconds(1));
        }
        /**
         * kafka 分区间offset是否重复,offset分区间独立，存在重复
         */
        static void check(){
            consumer.subscribe(Arrays.asList("my-partitioned-topic"));
            Set<TopicPartition> assignment = consumer.assignment();
            consumer.seekToBeginning(assignment);
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(1));
            for (final ConsumerRecord<Integer, String> record : records) {
                int partition = record.partition();
                long offset = record.offset();
                String value = record.value();
                System.out.printf("show message: partition %d offset %d message: %s\n", partition, offset, value);
            }
        }
        public static void main(String[] args) {
            //showOffsetFeatures(TOPIC_TEST, 0);
            //abountOffsetResetStrategy();
            //assignAfterPartitionDistributed();
            check();
        }
    }

    /**
     * 关于再均衡
     * 再均衡监听器 ConsumerRebalanceListener
     */
    static class AboutConsumerRebalance{
        static AtomicBoolean isRunning = new AtomicBoolean(true);
        static void consumerRebalanceListener(){
            //将消费进度缓存到本地，当发生再平衡时，执行commitSync提交消费位移，可以避免消息重复消费
            Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
            consumer.subscribe(Arrays.asList(TOPIC_TEST), new ConsumerRebalanceListener() {
                //触发时机：再均衡开始前 消费者停止读取消息之后，在此方法中进行消费位移的提交，可以避免重复消费问题
                //partitions 是再均衡前所分配到的分区
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    consumer.commitSync(currentOffsets);
                    currentOffsets.clear();
                }
                //触发时机：重新分配分区后 消费者开始读取消息前
                //partitions 表示再均衡后消费者分配到的分区
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    //do nothing
                }
            });
            try {
                while (isRunning.get()){
                    ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(1));
                    for (final ConsumerRecord<Integer, String> record : records) {
                        //process the record
                        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.offset() + 1);
                        currentOffsets.put(topicPartition, offsetAndMetadata);
                    }
                    consumer.commitAsync(currentOffsets, null);
                }
            }finally {
                consumer.close();
            }
        }
        public static void main(String[] args) {

        }
    }
    /**
     * 消费者拦截器 KafkaConsumerInterceptor 的使用
     */
    static class AboutConsumerInterceptor{
        /**
         * 实现一个消息的TTL功能，当消息时间戳距今超过 10s 就抛弃
         * 某条消息在既定的时间窗口内无法到达就被视为无效，不需要再继续处理
         */
        public static class CustomerInterceptorTTL implements ConsumerInterceptor<String, String> {
            private static final long EXPIRE_INTERVAL = 10 * 1000;
            @Override
            public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
                //收集每个分区过滤后的消息
                Map<TopicPartition, List<ConsumerRecord<String, String>>> newRecords = new HashMap<>();
                Set<TopicPartition> partitions = records.partitions();
                for (final TopicPartition partition : partitions) {
                    //当前遍历的分区对应的消息集
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    List<ConsumerRecord<String,String>> newTpRecords = new ArrayList<>();
                    for (final ConsumerRecord<String, String> topicRecord : partitionRecords) {
                        long l = System.currentTimeMillis() - topicRecord.timestamp();
                        if (l < EXPIRE_INTERVAL){
                            newTpRecords.add(topicRecord);
                        }
                    }
                    if (!newTpRecords.isEmpty()){
                        newRecords.put(partition, newTpRecords);
                    }
                }
                return new ConsumerRecords<>(newRecords);
            }
            @Override
            public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
                offsets.forEach((tp, offsetMeta) -> {
                    System.out.printf("%s : %s \n", tp.toString(), offsetMeta.offset());
                });
            }
            @Override
            public void close() {}
            @Override
            public void configure(Map<String, ?> configs) {}
        }
        public static void main(String[] args) throws Exception{
            consumer.subscribe(Arrays.asList(TOPIC_TEST));
            while (true){
                ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMinutes(3));
                for (final ConsumerRecord<Integer, String> record : records) {
                    System.out.println(record.value());
                }
                consumer.commitSync();
            }

            //COUNT_DOWN_LATCH.await(3, TimeUnit.MINUTES);
        }
    }

    /**
     * 多线程消费Kafka消息，由于KafkaConsumer线程不安全，如何进行编码
     */
    static class MultiThreadKafkaConsumer{
        static class KafkaConsumerThread extends Thread{
            private KafkaConsumer<Integer, String> kafkaConsumer;
            public KafkaConsumerThread(String topic, Properties properties){
                this.kafkaConsumer = new KafkaConsumer<Integer, String>(properties);
                this.kafkaConsumer.subscribe(Arrays.asList(topic));
            }
            @Override
            public void run() {
                try {
                    while (true){
                        ConsumerRecords<Integer, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                        for (final ConsumerRecord<Integer, String> record : records) {
                            //process record
                        }
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }finally {
                    kafkaConsumer.close();
                }
            }
        }
        public static void main(String[] args) {
            //创建多个消费线程
            new KafkaConsumerThread(TOPIC_TEST, null).start();
        }
    }
    /**
     * 也可以单线程式拉取消息，多线程式地处理消息
     * 这种写法可以减少KafkaConsumer TCP连接消耗，但难于处理顺序消费消息的问题
     */
    static class MultiThreadMessageHandler{
        static class RecordHandler extends Thread{
            private Map<TopicPartition, OffsetAndMetadata> offsets;
            public final ConsumerRecords<Integer, String> records;
            public RecordHandler(ConsumerRecords<Integer, String> records, Map<TopicPartition, OffsetAndMetadata> offsets){
                this.records = records;
                this.offsets = offsets;
            }
            @Override
            public void run() {
                for (final ConsumerRecord<Integer, String> record : records) {
                    //process record
                }
                /*-=-=-=-=-=- 解决并发处理消息的offset提交问题 （offset需要设置为手动提交）-=-=-=-=-=-*/
                for (final TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<Integer, String>> partitionRecords = records.records(partition);
                    //process record 应注意捕获异常，防止消息丢失（其他partition正常消费后进行了提交）
                    long lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    synchronized (offsets){
                        if (!offsets.containsKey(partition)){
                            offsets.put(partition, new OffsetAndMetadata(lastConsumedOffset + 1));
                        }else {
                            long position = offsets.get(partition).offset();
                            if (position < lastConsumedOffset + 1){
                                offsets.put(partition, new OffsetAndMetadata(lastConsumedOffset + 1));
                            }
                        }
                    }
                }
            }
        }
        static class KafkaConsumerThread extends Thread{
            //引入offsets解决线程池处理多分区消息时的offset提交问题
            private Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            private KafkaConsumer<Integer,String> kafkaConsumer;
            private ExecutorService executorService;
            private int threadNumber;
            public KafkaConsumerThread(String topic, int threadNumber, Properties properties){
                kafkaConsumer = new KafkaConsumer<Integer, String>(properties);
                kafkaConsumer.subscribe(Arrays.asList(topic));
                //CallerRunPolicy 可以防止线程池总体消费能力跟不上poll()拉取的能力
                executorService = new ThreadPoolExecutor(threadNumber, threadNumber, 0L, TimeUnit.SECONDS,
                        new ArrayBlockingQueue<>(1024), new ThreadPoolExecutor.CallerRunsPolicy());
            }
            @Override
            public void run() {
                try {
                    while (true){
                        ConsumerRecords<Integer, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                        if (!records.isEmpty()){
                            executorService.submit(new RecordHandler(records, offsets));
                        }
                        /*-=-= 线程池处理消息时并发处理 offsets， 主线程中对offsets进行提交后重置 =-=-*/
                        synchronized (offsets){
                            if (!offsets.isEmpty()){
                                kafkaConsumer.commitSync(offsets);
                                offsets.clear();
                            }
                        }
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }finally {
                    kafkaConsumer.close();
                }
            }
        }
    }

    /**
     * 除了使用 kafka-topics 命令管理资源，还可以使用KafkaAdminClient调用API进行资源管理
     * 这样可以实现自己的集管理、监控、运维、告警为一体的Kafka平台
     * 旧版Kafka有 kafka.admin.AdminClient 和 kafka.admin.AdminUtils 实现Kafka管理功能，不推荐使用
     */
    static class AboutKafkaAdminClient{
        static AdminClient client = null;
        static {
            Properties properties = new Properties();
            properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
            client = KafkaAdminClient.create(properties);
        }
        //变相执行 kafka-topics (不支持 --bootstrap-server 而应使用 --zookeeper)
        static void describeTopic(){
            String[] options = new String[]{
                    "--zookeeper", "localhost:2181",
                    "--describe",
                    "--topic", "test-topic"
            };
            TopicCommand.main(options);
        /*
            Topic:test-topic	PartitionCount:1	ReplicationFactor:1	Configs:
	        Topic: test-topic	Partition: 0	Leader: 0	Replicas: 0	Isr: 0
        */
        }

        static void createTopic() throws Exception{
            //设置topicName、分区数、负载因子
            //可以通过 NewTopic 中的 replicasAssignments field 指定副本分配方案
            NewTopic newTopic = new NewTopic(TOPIC_TEST + "_2", 4, (short) 1);
            HashMap<String, String> configs = new HashMap<>();
            configs.put("cleanup.policy", "compact");
            newTopic.configs(configs);
            CreateTopicsResult result = client.createTopics(Collections.singletonList(newTopic));
            result.all().get();
            client.close();
            //Kafka commitId : 3402a8361b734732
        }

        public static void main(String[] args) throws Exception{
            //describeTopic();
            createTopic();
        }
    }

    public static void main(String[] args) {
        double ratio = 0.435;
        System.out.printf("[INFO] 当前发送成功率 %f %% %n", ratio * 100);
    }

}
