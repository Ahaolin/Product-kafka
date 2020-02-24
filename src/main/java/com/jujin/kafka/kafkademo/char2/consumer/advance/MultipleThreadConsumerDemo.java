package com.jujin.kafka.kafkademo.char2.consumer.advance;

import com.jujin.kafka.kafkademo.char2.consumer.advance.base.BaseConsumer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * 多线程进行消费消息
 * @author MSI
 * @date 2020年02月21日 19:17:33
 */
public class MultipleThreadConsumerDemo extends BaseConsumer {

    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "group.demo";

    public static Map<TopicPartition, OffsetAndMetadata> offsets = new ConcurrentHashMap<>(16);

    /**
     * 注意该方法的 提交方式 为 自动提交
     * 如果是方式一  直接改就完了 手动提交即可。
     * 如果是方式三  就有了问题。请看方式4
     */
    private static Properties initConfig(){
        return initConfig(true);
    }

    private static Properties initConfig(boolean flag){
        flag = Optional.ofNullable(flag).orElse(false);
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, flag);
        return props;
    }

    /**
     * 每个线程实例化一个 KafkaConsumer 对象
     * 一个消费实例可以消费一个或多个分区中的消息（消费线程），所有的消费线程都隶属于同一个消费组。这种实现方式的并发度受限于分区的实际个数。
     * 如果消费线程 > 分区数  存在部分线程一直空闲的状态。<br>
     *
     *<p>
     *  消费线程的数量由 consumerThreadNum 变量指定。
     *  一般一个主题的分区数事先可以知晓，可以将 consumerThreadNum 设置成不大于分区数的值，如果不知道主题的分区数，
     *  那么也可以通过 KafkaConsumer 类的 partitionsFor() 方法来间接获取，进而再设置合理的 consumerThreadNum 值。
     *</p>
     * 缺点：系统开销很大
     */
    public static void firstMultipleThreadDemo(){
        Properties props = initConfig();
        int consumerThreadNum = 4;
        for(int i=0;i<consumerThreadNum;i++) {
            new KafkaConsumerThread1(props,topic).start();
        }
    }

    /**
     * 多个消费线程同时消费同一个分区，这个通过 assign()、seek() 等方法实现,
     * 打破原有的消费线程的个数不能超过分区数的限制，进一步提高了消费的能力。
     *
     * 缺点：对于位移提交和顺序控制的处理就会变得非常复杂，实际应用中使用得极少
     * 一般而言，分区是消费线程的最小划分单位。
     */
    public static void secondMultipleThreadDemo(){
        // 暂无实现
    }

    /**
     * 相比较而言 方式1，消费消息时，时间不确定，可能导致poll()方法变慢 进而造成整体消费性能的下降
     *
     * 第三种实现方式相比第一种实现方式{@link MultipleThreadConsumerDemo#firstMultipleThreadDemo()}而言，
     * 除了横向扩展的能力，还可以减少TCP连接对系统资源的消耗，不过缺点就是对于消息的顺序处理就比较困难了。
     */
    public static void thirdMultipleThreadDemo(){
        Properties props = initConfig();
        KafkaConsumerThread3 consumerThread = new KafkaConsumerThread3(props, topic, Runtime.getRuntime().availableProcessors());
        consumerThread.start();
    }


    /**
     * 相比较第三种方式{@link MultipleThreadConsumerDemo#thirdMultipleThreadDemo()}：无法使用手工提交做了修改。此时仍然无法使用手工提交，
     *
     * <p>这里引入一个共享变量<b>offsets</b>  来参与提交。</p>
     * <p>
     *     每一个处理消息的 RecordHandler 类在处理完消息之后都将对应的消费位移保存到共享变量 offsets 中，
     *     KafkaConsumerThread 在每一次 poll() 方法之后都读取 offsets 中的内容并对其进行位移提交。
     *     注意在实现的过程中对 offsets 读写需要加锁处理，防止出现并发问题。并且在写入 offsets 的时候需要注意位移覆盖的问题
     * </p>
     *
     * 注意：
     * <p>
     *  其实这种位移提交的方式会有数据丢失的风险。对于同一个分区中的消息，假设一个处理线程 RecordHandler1 正在处理 offset 为0～99的消息，
     *  而另一个处理线程 RecordHandler2 已经处理完了 offset 为100～199的消息并进行了位移提交，此时如果 RecordHandler1 发生异常，
     *  则之后的消费只能从200开始而无法再次消费0～99的消息，从而造成了消息丢失的现象。
     *  这里虽然针对位移覆盖做了一定的处理，但还没有解决异常情况下的位移覆盖问题。 {@link MultipleThreadConsumerDemo#fifthMultipleThreadDemo()}
     * </p>
     */
    public static void forthMultipleThreadDemo(){
        Properties props = initConfig();
        KafkaConsumerThread4 consumerThread = new KafkaConsumerThread4(props, topic, Runtime.getRuntime().availableProcessors());
        consumerThread.start();
    }


    /**
     * @serialData  char14 滑动窗口
     */
    public static void fifthMultipleThreadDemo(){
        // do noting
    }

    public static void main(String[] args) {
        firstMultipleThreadDemo();
        thirdMultipleThreadDemo();
        forthMultipleThreadDemo();
    }

}




/*****************************方法1  ： 线程辅助类 start*********************************/
class KafkaConsumerThread1 extends Thread{
    private KafkaConsumer<String, String> kafkaConsumer;

    public KafkaConsumerThread1(Properties props, String topic) {
        this.kafkaConsumer = new KafkaConsumer<>(props);
        this.kafkaConsumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run(){
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    //处理消息模块	①
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaConsumer.close();
        }
    }
}
/*****************************方法1  ： 线程辅助类 end*********************************/





/*****************************方法3  ： 线程辅助类 start*********************************/

/**
 * 一个消费线程
 * 需要注意线程池 策略选择了CallerRunsPolicy() 可以防止线程池的总体消费能力跟不上 poll() 拉取的能力，从而导致异常现象的发生
 */
class KafkaConsumerThread3 extends Thread {
    private KafkaConsumer<String, String> kafkaConsumer;
    private ExecutorService executorService;
    private int threadNumber;

    public KafkaConsumerThread3(Properties props, String topic, int threadNumber) {
        kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        this.threadNumber = threadNumber;
        executorService = new ThreadPoolExecutor(threadNumber, threadNumber,
                0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty()) {
                    executorService.submit(new RecordsHandler3(records));
                }	 // ①
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaConsumer.close();
        }
    }

}

/**
 * 处理消息的
 */
class RecordsHandler3 extends Thread{
    public final ConsumerRecords<String, String> records;

    public RecordsHandler3(ConsumerRecords<String, String> records) {
        this.records = records;
    }

    @Override
    public void run(){
        //处理records.
    }
}
/*****************************方法3  ： 线程辅助类 end*********************************/



/*****************************方法4  ： 线程辅助类 start*********************************/

/**
 * 一个消费线程
 * 需要注意线程池 策略选择了CallerRunsPolicy() 可以防止线程池的总体消费能力跟不上 poll() 拉取的能力，从而导致异常现象的发生
 */
class KafkaConsumerThread4 extends Thread {
    private KafkaConsumer<String, String> kafkaConsumer;
    private ExecutorService executorService;
    private int threadNumber;

    public KafkaConsumerThread4(Properties props, String topic, int threadNumber) {
        kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        this.threadNumber = threadNumber;
        executorService = new ThreadPoolExecutor(threadNumber, threadNumber,
                0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty()) {
                    executorService.submit(new RecordsHandler4(records));
                }	 // ①
                synchronized (MultipleThreadConsumerDemo.offsets) {
                    if (!MultipleThreadConsumerDemo.offsets.isEmpty()) {
                        kafkaConsumer.commitSync(MultipleThreadConsumerDemo.offsets);
                        MultipleThreadConsumerDemo.offsets.clear();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaConsumer.close();
        }
    }

}

/**
 * 处理消息的
 */
class RecordsHandler4 extends Thread{
    public final ConsumerRecords<String, String> records;

    public RecordsHandler4(ConsumerRecords<String, String> records) {
        this.records = records;
    }

    @Override
    public void run(){
        //处理records.
        for (TopicPartition tp : records.partitions()) {
            List<ConsumerRecord<String, String>> tpRecords = records.records(tp);
            //处理tpRecords.
            long lastConsumedOffset = tpRecords.get(tpRecords.size() - 1).offset();
            synchronized (MultipleThreadConsumerDemo.offsets) {
                if (!MultipleThreadConsumerDemo.offsets.containsKey(tp)) {
                    MultipleThreadConsumerDemo.offsets.put(tp, new OffsetAndMetadata(lastConsumedOffset + 1));
                }else {
                    long position = MultipleThreadConsumerDemo.offsets.get(tp).offset();
                    if (position < lastConsumedOffset + 1) {
                        MultipleThreadConsumerDemo.offsets.put(tp, new OffsetAndMetadata(lastConsumedOffset + 1));
                    }
                }
            }
        }
    }
}
/*****************************方法4  ： 线程辅助类 end*********************************/