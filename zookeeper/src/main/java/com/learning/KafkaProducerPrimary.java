package com.learning;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.learning.KafkaConfig.BOOTSTRAP_SERVERS;
import static com.learning.KafkaConfig.TOPIC_TEST;

public class KafkaProducerPrimary {
    private static final CountDownLatch COUNT_DOWN_LATCH = new CountDownLatch(1);

    private static final org.apache.kafka.clients.producer.KafkaProducer<Integer, String> producer;
    static {
        Properties props = new Properties();
        //Kafka提供了很多常量供使用
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        //如果不指定client.id， kafka会生成一个类似 producer-1 形式的字符串
        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, "DemoProducer");
        //发往broker经网络传输需要对消息做序列化
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
        //避免拼写错误的办法
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //自定义的分区器(比如商品消息的分区对应商品所属仓库的编号)
        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "xxx");
        //KafkaProducer 是线程安全，可以池化后进行使用
        producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
    }

    static class Producer{
        private final Boolean isAsync;
        private String topic;

        public Producer(Boolean isAsync, String topic) {
            this.isAsync = isAsync;
            this.topic = topic;
        }

        public void produce(Integer msgKey, String message){
            String msg = "Message_"+ message;
            if (isAsync){// 异步发送(async)，需要传递回调函数(回调函数也是分区有序的！)
                producer.send(new ProducerRecord<>(topic, msgKey, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        System.out.println("exception: "+ ((e == null)?"":e.getMessage()));
                        System.out.println("#offset: "+ recordMetadata.offset());
                    }
                });
            }else {
                try { //同步发送(sync)，阻塞等待
                    producer.send(new ProducerRecord<>(topic, msgKey, msg)).get(5, TimeUnit.SECONDS);
                } catch (Throwable e) {
                }
            }
            //其实还有第三种发后即忘的写法(fire and forget) 发送性能高但可靠性差
            //producer.send(new ProducerRecord<>(topic, msgKey, msg));
            //producer.close(1, TimeUnit.MINUTES);//实际中使用不带超时时间的close方法比较多
        }
        private static final long EXPIRE_INTERVAL = 10 * 1000;

        static void sendMessageAndCheckPartitionDistribution() throws Exception{
            Producer client = new Producer(true, "my-partitioned-topic");
            client.produce(11,"kafka producer sending message,show offset");
            client.produce(13, "about offset strategy");
            client.produce(14, "this topic has 3 partitions");
            client.produce(15, "these message should be distributed into different partition equally");
            client.produce(16, "then we could consumer message continuously");
            client.produce(17, "previous offset was all 2, but got nothing from partitions");
            client.produce(18, "maybe log clearing has been performed before");
            COUNT_DOWN_LATCH.await(10, TimeUnit.SECONDS);
        }
        static void testCustomerInterceptor() throws Exception{
            ProducerRecord<Integer, String> record = new ProducerRecord<>(TOPIC_TEST, 0, System.currentTimeMillis(), null, "send valid message");
            ProducerRecord<Integer, String> timeoutRecord = new ProducerRecord<>(TOPIC_TEST, 0, System.currentTimeMillis() - EXPIRE_INTERVAL, null, "invalid timeout message will be ignored");
            ProducerRecord<Integer, String> record2 = new ProducerRecord<>(TOPIC_TEST, 0, System.currentTimeMillis(), null, "second valid message");
            producer.send(record);
            producer.send(timeoutRecord);
            producer.send(record2);
            COUNT_DOWN_LATCH.await(10, TimeUnit.SECONDS);
        }
        public static void main(String[] args) throws Exception {
            testCustomerInterceptor();
        }
    }
}
