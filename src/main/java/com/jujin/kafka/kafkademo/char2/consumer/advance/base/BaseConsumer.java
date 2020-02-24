package com.jujin.kafka.kafkademo.char2.consumer.advance.base;

import com.jujin.kafka.kafkademo.char2.consumer.KafkaConsumerAnalysis;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author MSI
 * @date 2020年02月20日 14:49:49
 */
public abstract class BaseConsumer {

    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    /**
     * 配置Kafka
     * @return
     */
    public static Properties initOffSetConfig(boolean isAutoCommit){
        Properties props = KafkaConsumerAnalysis.initConfig();
        // 开启手动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, isAutoCommit);
        return props;
    }

    /**
     * 初始化 kafka客户端 并且已经订阅了
     */
    public static KafkaConsumer<String, String> init(){
        Properties props = initOffSetConfig(false);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        KafkaConsumerAnalysis.subscribe(consumer);
        return consumer;
    }

    /**
     * 初始化 kafka客户端 必须手动调用了<b>subscribe()</b>
     * {@link BaseConsumer#init()}
     */
    public static KafkaConsumer<String, String> initNotSuB(){
        Properties props = initOffSetConfig(false);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
//        KafkaConsumerAnalysis.subscribe(consumer);
        return consumer;
    }
}
