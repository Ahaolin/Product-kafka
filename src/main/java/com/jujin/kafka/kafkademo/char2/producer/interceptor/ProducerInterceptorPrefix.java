package com.jujin.kafka.kafkademo.char2.producer.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author MSI
 * 生产者拦截器示例
 *  ProducerInterceptorPrefix 中通过 onSend() 方法来为每条消息添加一个前缀“prefix1-”，
 *  并且通过 onAcknowledgement() 方法来计算发送消息的成功率。
 *  指定拦截器 ： properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,ProducerInterceptorPrefix.class.getName());
 */
public class ProducerInterceptorPrefix implements ProducerInterceptor<String, String> {
    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;

    /**
     * KafkaProducer 在将消息序列化和计算分区之前会调用生产者拦截器的 onSend() 方法来对消息进行相应的定制化操作。
     * 一般来说最好不要修改消息 ProducerRecord 的 topic、key 和 partition 等信息
     */
    @Override
    public ProducerRecord<String, String> onSend(
            ProducerRecord<String, String> record) {
        String modifiedValue = "prefix1-" + record.value();
        return new ProducerRecord<>(record.topic(),
                record.partition(), record.timestamp(),
                record.key(), modifiedValue, record.headers());
    }

    /**
     * KafkaProducer 会在消息被应答（Acknowledgement）之前或消息发送失败时调用生产者拦截器的 onAcknowledgement() 方法，
     * 优先于用户设定的 Callback 之前执行
     * @param recordMetadata
     * @param e
     */
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e == null) {
            sendSuccess++;
        } else {
            sendFailure++;
        }
    }

    /**
     * close() 方法主要用于在关闭拦截器时执行一些资源的清理工作
     */
    @Override
    public void close() {
        double successRatio = (double) sendSuccess / (sendFailure + sendSuccess);
        System.out.println("[INFO] 发送成功率="
                + String.format("%f", successRatio * 100) + "%");
    }

    /**
     * 以上3个方法中抛出的异常都会被捕获并记录到日志中，但并不会再向上传递。
     */

    @Override
    public void configure(Map<String, ?> map) {
    }
}
