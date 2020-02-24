package com.jujin.kafka.kafkademo.char2.consumer.advance;

import com.jujin.kafka.kafkademo.char2.consumer.advance.base.BaseConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 *消息的拉取是根据 poll() 方法中的逻辑来处理的，无法精确地掌控其消费的起始位置。提供的 auto.offset.reset 参数也只能在找不到消费位移或位移越界的情况下粗粒度地从开头或末尾开始消费
 * 所以<b>更细粒度的掌控，可以让我们从特定的位移处开始拉取消息。</b>
 * @author MSI
 * @date 2020年02月20日 14:41:53
 * 拉取 消息
 */
@Slf4j
public class PollPlusConsumerDemo extends BaseConsumer {

    private static final String TOPIC = "topic-demo";

    /**
     * poll()方法时长见{@link PollPlusConsumerDemo#normPollTime()}
     */
    public static void text(){
        KafkaConsumer<String, String> consumer = initNotSuB();
        consumer.subscribe(Arrays.asList(TOPIC));
        /**
         *  如果poll(0) 的话，seek() 方法并未有任何作用。
         *  因为当 poll() 方法中的参数为0时，此方法立刻返回，那么 poll() 方法内部进行分区分配的逻辑就会来不及实施
         *  消费者此时并未分配到任何分区，如此第②行中的 assignment 便是一个空列表
         */
        consumer.poll(Duration.ofMillis(10000)); // ①
        Set<TopicPartition> assignment = consumer.assignment(); // ② 获取消费者所分配到的分区信息的
        for (TopicPartition partition : assignment) {
            /**
             * seek() partition表示分区，而offSet参数用来指定从分区的哪个位置开始消费。
             * seek() 方法只能重置消费者分配到的分区的消费位置，而分区的分配是在 poll() 方法的调用过程中实现的。
             * 执行 <b>seek() 方法之前需要先执行一次 poll() 方法</b>，等到分配到分区之后才可以重置消费位置
             */
            consumer.seek(partition, 10); // ③
        }
        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));
            //consume the record.
        }
    }

    /**
     * 参考 seek()方法
     *  设置Poll()方法的时长
     */
    public static void normPollTime(){
        KafkaConsumer<String, String> consumer = initNotSuB();
        consumer.subscribe(Arrays.asList(TOPIC));
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {//如果不为0，则说明已经成功分配到了分区
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }
        for (TopicPartition tp : assignment) {
            consumer.seek(tp, 10);
        }
        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));
            //consume the record.
        }
    }


    // 具体请看12章节
    public static void main(String[] args) {

    }

}
