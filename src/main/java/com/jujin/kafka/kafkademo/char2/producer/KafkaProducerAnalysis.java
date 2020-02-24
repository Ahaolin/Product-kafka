package com.jujin.kafka.kafkademo.char2.producer;

import com.jujin.kafka.kafkademo.char2.entity.Company;
import com.jujin.kafka.kafkademo.char2.entity.WrapperProperties;
import com.jujin.kafka.kafkademo.char2.producer.interceptor.ProducerInterceptorPrefix;
import com.jujin.kafka.kafkademo.char2.producer.interceptor.ProducerInterceptorSupperPrefix;
import com.jujin.kafka.kafkademo.char2.producer.partition.DemoPartitioner;
import com.jujin.kafka.kafkademo.char2.producer.serializer.CompanySerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author MSI
 * 代码清单3-1 生产者客户端示例代码
 */
public class KafkaProducerAnalysis {

    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic-demo";

    /**
     * 简单的Demo
     * @return
     */
    public static WrapperProperties<String> initConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("client.id", "producer.client.id.demo");
        return new WrapperProperties<>(props," ");
    }

    /**
     * 标准的简单Demo
     * @return
     */
    public static WrapperProperties<String> initConfig1() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        return new WrapperProperties<>(props," ");
    }


    /**
     * 使用自定义的序列化
     */
    public static WrapperProperties<Company> initConfig2() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                CompanySerializer.class.getName());
        properties.put("bootstrap.servers", brokerList);
        Company company = Company.builder().name("hiddenkafka")
                .address("China").build();
        return new WrapperProperties<>(properties,company);
    }

    /**
     * 开始执行
     * @param args
     */
    public static void main(String[] args) {
        WrapperProperties wrapperProperties = initConfig2();
        Properties props = wrapperProperties.getProps();
//        props.put(ProducerConfig.RETRIES_CONFIG, 10); //对于可重试异常 规定的重试次数内自行恢复了，就不会抛出异常
//        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DemoPartitioner.class.getName()); // 使用自定义的分区器

//        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptorPrefix.class.getName()); // 指定拦截器
        /**
         * 拦截链
         *  在拦截链中，如果某个拦截器执行失败，那么下一个拦截器会接着从上一个执行成功的拦截器继续执行。
         */
//        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptorPrefix.class.getName() + "," + ProducerInterceptorSupperPrefix.class.getName());

        if (wrapperProperties.getT() instanceof  String) {
            KafkaProducer<String, String> producer = new KafkaProducer<>(props);
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, "Hello, Kafka!");
            setMessage(producer, record);
        }else if (wrapperProperties.getT() instanceof  Company){
            Company company = (Company) wrapperProperties.getT();
            KafkaProducer<String, Company> producer =
                    new KafkaProducer<>(props);
            ProducerRecord<String, Company> record =
                    new ProducerRecord<>(topic,company);
            setMessage(producer, record);
        }
    }


    /**
     * 发送消息的三种方式
     * @param producer
     * @param record
     */
    private static void setMessage(KafkaProducer producer, ProducerRecord record) {
        try {
            producer.send(record); // 发后即忘 无需确定是否到达
//            producer.send(record).get(); // 同步发送 阻塞等待 Kafka 的响应，直到消息发送成功，或者发生异常。如果发生异常，那么就需要捕获异常并交由外层逻辑处理
            producer.send(record, (metadata, exception) -> { // 异步发送 Kafka 在返回响应时调用该函数来实现异步的发送确认。
                // 注意 两个参数是互斥 消息成功（metadata不为null,exception为null） 否则（metadata为null,exception不为null）
                if (exception != null) {
                    // 实际应用中应该使用更加稳妥的方式来处理，比如可以将异常记录以便日后分析，也可以做一定的处理来进行消息重发
                    exception.printStackTrace();
                } else {
                    System.out.println(metadata.topic() + "-" +
                            metadata.partition() + ":" + metadata.offset());
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
