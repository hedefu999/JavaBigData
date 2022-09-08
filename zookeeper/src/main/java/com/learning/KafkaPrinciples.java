package com.learning;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class KafkaPrinciples {
    /**
     * 学习消费组内消费者的分区分配策略时接触了PartitionAssignor
     * Kafka提供了一个抽象实现 AbstractPartitionAssignor
     * 书中提供了一个随机分区策略的实现例子
     * 这个分配策略的使用方法是，在消费者客户端配置
     @see org.apache.kafka.clients.consumer.ConsumerConfig PARTITION_ASSIGNMENT_STRATEGY_CONFIG
     properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RandomAssignor.class.getName())
     */
    static class RandomAssignor extends AbstractPartitionAssignor{
        @Override
        public String name() {
            return "random";
        }
        @Override
        public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                        Map<String, Subscription> subscriptions) {
            Map<String, List<String>> consumersPerTopic = consumerPerTopic(subscriptions);
            HashMap<String, List<TopicPartition>> assignment = new HashMap<>();
            for (final String memberId : subscriptions.keySet()) {
                assignment.put(memberId, new ArrayList<>());
            }
            //针对每一个主题进行分区分配
            for (final Map.Entry<String, List<String>> topicEntry : consumersPerTopic.entrySet()) {
                String topic = topicEntry.getKey();
                List<String> consumersForTopic = topicEntry.getValue();
                int consumerSize = consumersForTopic.size();
                Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
                if (numPartitionsForTopic == null){
                    continue;
                }
                //当前主题下的所有分区
                List<TopicPartition> partitions = AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic);
                //将每一个分区随机分配给消费者
                for (final TopicPartition partition : partitions) {
                    int rand = new Random().nextInt(consumerSize);
                    String randConsumerId = consumersForTopic.get(rand);
                    assignment.get(randConsumerId).add(partition);
                }
            }
            return assignment;
        }
        //收集每个主题对应的消费者列表，即 [topic,List<consumer>]
        private Map<String,List<String>> consumerPerTopic(Map<String,Subscription> consumerMetadata){
            Map<String, List<String>> res = new HashMap<>();
            for (final Map.Entry<String, Subscription> subscriptionEntry : consumerMetadata.entrySet()) {
                String consumerId = subscriptionEntry.getKey();
                for (final String topic : subscriptionEntry.getValue().topics()) {
                    put(res, topic, consumerId);
                }
            }
            return res;
        }

    }

    /**
     * 通过自定义分区分配策略改变kafka的默认消费逻辑：
     * 一个分区只能分配到同一消费组中的一个消费者
     * 实现一个广播式的分配方式，组内的每个消费者消费全部分区，这样一个分区就可以分配给组内的n个消费者进行消费
     */
    static class BroadcastAssignor extends AbstractPartitionAssignor{
        @Override
        public String name() {
            return "broadcast";
        }
        @Override
        public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                        Map<String, Subscription> subscriptions) {
            Map<String, List<String>> consumersPerTopic = consumersPerTopic(subscriptions);
            Map<String, List<TopicPartition>> assignment = new HashMap<>();
            subscriptions.keySet().forEach(memberId ->
                    assignment.put(memberId, new ArrayList<>()));
            //针对每一个topic，为每一个订阅的consumer分配所有的分区，允许一个分区分配到了多个consumer
            consumersPerTopic.entrySet().forEach(topicEntry -> {
                String topic = topicEntry.getKey();
                List<String> members = topicEntry.getValue();
                Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
                if (numPartitionsForTopic == null || members.isEmpty()){
                    return;
                }
                List<TopicPartition> partitions = AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic);
                if (! partitions.isEmpty()){
                    members.forEach(memberId -> {//分配全部分区关键在于这个addAll方法
                        assignment.get(memberId).addAll(partitions);
                    });
                }
            });
            return assignment;
        }
        private Map<String,List<String>> consumersPerTopic(Map<String, Subscription> consumerMetadata){
            Map<String, List<String>> res = new HashMap<>();
            for (final Map.Entry<String, Subscription> subscriptionEntry : consumerMetadata.entrySet()) {
                String consumerId = subscriptionEntry.getKey();
                for (final String topic : subscriptionEntry.getValue().topics()) {
                    put(res, topic, consumerId);
                }
            }
            return res;
        }
        /**
         * 这种组内广播会存在一个严重的问题：默认的offset提交会失效
         * 所有consumer都会提交自己的offset到 _consumer_offsets 中，相互覆盖
         * 当有consumer上线下线时由于offset被其他consumer修改，会产生消息丢失和消息重复的情况
         * 解决办法是每个consumer自己保存自身的offset，如存放到本地文件或数据库中
         */

    }
}
