package com.jujin.kafka.kafkademo.char2.consumer;

import com.jujin.kafka.kafkademo.char2.consumer.deserializer.CompanyDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * @author MSI
 * //代码清单8-1 消费者客户端示例
 * 点对点模式 ： 所有消费者在一个消费组
 * 发布/订阅模式：所有消费者分别在一个消费组
 * 核心 ： 同一个分区的消息在一个消费组只会被同一个消费者消费，消费组之间互补影响
 */
@Slf4j
public class KafkaConsumerAnalysis {
    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "group.demo";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static final Pattern PATTERN =  Pattern.compile("topic-.*");

    public static Properties initConfig(){
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "client.id.demo");
        return props;
    }

    /**
     * 使用assign实现订阅主题（全部分区）的功能  = consumer.subscribe(Arrays.asList(topic));
     * 任选其一
     * 注意：这三种状态是互斥的 一个消费者中只能存在一种 不可为NONE(无订阅) 否则{@link IllegalStateException}
     */
    public static void subscribe(KafkaConsumer<String, String> consumer){
        /**
         * 全订阅1 - 指定分区的订阅方式（USER_ASSIGNED）
         */
        List<TopicPartition> partitions = new ArrayList<>();
        // 查看所有分区
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        if (partitionInfos != null) {
            for (PartitionInfo tpInfo : partitionInfos) {
                partitions.add(new TopicPartition(tpInfo.topic(), tpInfo.partition()));
            }
        }
        consumer.assign(partitions);

        /**
         * 全订阅2 - 集合订阅 （AUTO_TOPICS）
         */
        consumer.subscribe(Arrays.asList(topic));

        /**
         * 正则表达式进行匹配订阅 - 正则表达式订阅（AUTO_PATTERN ）
         */
        consumer.subscribe(PATTERN);

        /**
         * 只订阅 topic-demo 主题中分区编号为0的分区 指定分区的订阅方式（USER_ASSIGNED）
         */
        consumer.assign(Arrays.asList(new TopicPartition(topic, 0)));
    }

    /**
     * 取消订阅
     * 任选其一
     */
    public static void unsubscribe(KafkaConsumer<String, String> consumer){
        consumer.unsubscribe();
        consumer.subscribe(new ArrayList<String>());
        consumer.assign(new ArrayList<TopicPartition>());
    }

    /**
     * 消费消息
     */
    public static void getConsumerMessage(ConsumerRecords<String, String> records) {
        /**
         * demo1 简单消费
         */
        records.forEach((record)->{
            System.out.println("topic = " + record.topic() + ", partition = "+ record.partition() + ", offset = " + record.offset());
            System.out.println("key = " + record.key() + ", value = " + record.value());
            //do something to process record.
        });

        /**
         * demo2 获取消息集中指定分区
         */
        records.partitions().forEach(partition->{
            // partition 分区
            List<ConsumerRecord<String, String>> recordList = records.records(partition); // 指定分区的消息
            recordList.parallelStream().forEach(System.out::println);
        });

        /**
         * demo3 按照主题维度来进行消费
         * 需要自己获取订阅的主题 {@link KafkaConsumerAnalysis#subscribe(org.apache.kafka.clients.consumer.KafkaConsumer)}
         */
        List<String> topicList = Arrays.asList(topic);
        topicList.forEach((topic)->{
            for (ConsumerRecord<String, String> record : records.records(topic)) {
                System.out.println(record.topic() + " : " + record.value());
            }
        });

    }

    public static void main(String[] args) {
        Properties props = initConfig();
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CompanyDeserializer.class.getName()); 自定义编码器
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        subscribe(consumer);
//        unsubscribe(consumer); // 取消全订阅
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                getConsumerMessage(records);
            }
        } catch (Exception e) {
            log.error("occur exception ", e);
        } finally {
            consumer.close();
        }
    }

    /**
     * 一个完成的消费者程序参考
     *  关闭这个消费逻辑的时候，可以调用 consumer.wakeup()，也可以调用 isRunning.set(false)。
     */
    public void defaultConsumer(KafkaConsumer<String, String> consumer){
        consumer.subscribe(Arrays.asList(topic));
        try {
            while (isRunning.get()) {
                //consumer.poll(***)
                //process the record.
                //commit offset.
            }
        } catch (WakeupException e) {
            // ingore the error
        } catch (Exception e){
            // do some logic process.
        } finally {
            // maybe commit offset.
            consumer.close();
        }
    }
}
