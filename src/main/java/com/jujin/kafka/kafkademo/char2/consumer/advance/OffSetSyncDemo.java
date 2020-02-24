package com.jujin.kafka.kafkademo.char2.consumer.advance;

import com.jujin.kafka.kafkademo.char2.consumer.advance.base.BaseConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

/**
 * @author MSI
 * 同步位移提交
 *
 * 对于消息在分区中的位置，我们将 offset 称为“偏移量”；
 * 对于消费者消费到的位置，将 offset 称为“位移”
 * 当然，对于一条消息而言，它的偏移量和消费者消费它时的消费位移是相等的
 */
@Slf4j
public class OffSetSyncDemo extends BaseConsumer {

    /********************************************************************************************/

    /**
     * 同步提交
     * 重复消费的问题
     *   如果在业务逻辑处理完之后，并且在同步位移提交前，程序出现了崩溃，
     *   那么待恢复之后又只能从上一次位移提交的地方拉取消息，由此在两次位移提交的窗口中出现了重复消费的现象。
     *
     *   commitSync() 方法会根据 poll() 方法拉取的最新位移来进行提交（注意提交的值对应于本节第1张图中 position 的位置），
     *   只要没有发生不可恢复的错误（Unrecoverable Error），它就会阻塞消费者线程直至位移提交完成。对于不可恢复的错误，
     *   比如 CommitFailedException、WakeupException、InterruptException、AuthenticationException、AuthorizationException 等，
     *   我们可以将其捕获并做针对性的处理。
     */

    /**
     * 手动位移提交测试
     * 拉取到的每一条消息做相应的逻辑处理，然后对整个消息集做同步提交
     */
    public static void handSubmitSync() {
        try( KafkaConsumer<String, String> consumer = init();) {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    //do some logical processing.
                }
                consumer.commitSync();
            }
        }
    }

    /**
     * 手动位移批量提交测试
     * 拉取到的消息存入缓存 buffer，等到积累到足够多的时候，也就是示例中大于等于200个的时候，再做相应的批量处理，之后再做批量提交
     */
    public static void handBatchSubmitSync() {
        try( KafkaConsumer<String, String> consumer = init();) {
            final int minBatchSize = 200;
            List<ConsumerRecord> buffer = new ArrayList<>();
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    buffer.add(record);
                }
                if (buffer.size() >= minBatchSize) {
                    //do some logical processing with buffer.
                    consumer.commitSync();
                    buffer.clear();
                }
            }
        }
    }

    /********************************************************************************************/


    /**
     * 含参的同步位移提交
     * 无参的 commitSync() 方法只能提交当前批次对应的 position 值。
     * 如果需要提交一个中间值，比如业务每消费一条消息就提交一次位移,可采取以下方法
     *
     * 实战这 不建议使用 会将性能拉到一个相当低的点
     */
    public static void handSubmitSyncParam() {
        try( KafkaConsumer<String, String> consumer = init();){
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    //do some logical processing.
                    long offset = record.offset();
                    TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(offset + 1)));
                }
            }
        }
    }

    /**
     *按分区粒度同步提交消费位移
     * 相较于{@link OffSetSyncDemo#handSubmitSyncParam()} 方法 更多时候 我们提交<b>按照分区的粒度划分提交位移的界限</b>
     */
    public static void handSubmitSyncPartition(){
        try (KafkaConsumer<String, String> consumer = init();){
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        //do some logical processing.
                    }
                    long lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastConsumedOffset + 1)));
                }
            }
        }
    }

    public static void main(String[] args) {
        /**
         * 手动唯一提交
         */
        handSubmitSync();

        /**
         * 手动批量位移提交
         */
        handBatchSubmitSync();

        /**
         * 手动位移提交 指定位移
         */
        handSubmitSyncParam();

        /**
         * 手动位移提交 分区力度
         */
        handSubmitSyncPartition();

        /**
         * 异步位移提交
         */
        handSubmitAsync();
    }

    /**
     * 异步位移提交
     */
    public static void handSubmitAsync(){
        try(KafkaConsumer<String, String> consumer = init()){
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    //do some logical processing.
                }
                /**
                 * 当位移提交完成后会回调 OffsetCommitCallback 中的 onComplete() 方法
                 */
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
                                           Exception exception) {
                        if (exception == null) {
                            System.out.println(offsets);
                        }else {
                            log.error("fail to commit offsets {}", offsets, exception);
                        }
                    }
                });
            }
        }
    }


}
