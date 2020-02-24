package com.jujin.kafka.kafkademo.char3;

import org.apache.kafka.clients.admin.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * 初时 KafaKaAdminClient
 *
 * @author MSI
 * @date 2020年02月24日 22:45:24
 */
public class KafkaAdminClientStartDemo {

    private static final String BROKE_LIST = "localhost:9092";
    private static final String TOPIC = "topic-admin";

    /**
     * 主题设置分区的方案
     * 为true  通过指定分区数和副本因子来创建一个主题
     * 为false 通过指定区副本的具体分配方案来创建一个主题
     */
    private static final Boolean PATTERN_TOPIC = Boolean.TRUE;

    public static Properties initConfig() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BROKE_LIST);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        return props;
    }

    /**
     * 创建NewTopic对象 并配置一些属性
     */
    private static NewTopic buildNewTopic() {
        NewTopic newTopic;
        if (PATTERN_TOPIC) {
            // 通过指定分区数和副本因子来创建一个主题
            /**
             * NewTopic 字段对应创建主题的具体信息 ：
             * private final String name                                        ： 主题名称
             * private final int numPartitions;                                 ： 分区数
             * private final short replicationFactor                            ：  副本因子
             * private final Map<Integer, List<Integer>> replicasAssignments    ： 分配方案
             * private Map<String, String> configs = null                       ：  配置
             */
            newTopic = new NewTopic(TOPIC, 4, (short) 1); //	设定所要创建主题的具体信息
        } else {
            // 通过指定区副本的具体分配方案来创建一个主题
            Map<Integer, List<Integer>> replicasAssignments = new HashMap<>();
            replicasAssignments.put(0, Arrays.asList(0));
            replicasAssignments.put(1, Arrays.asList(0));
            replicasAssignments.put(2, Arrays.asList(0));
            replicasAssignments.put(3, Arrays.asList(0));
            newTopic = new NewTopic(TOPIC, replicasAssignments);
        }

        // 如果创建主题时指定需要覆盖的配置
        Map<String, String> configs = new HashMap<>();
        configs.put("cleanup.policy", "compact");
        newTopic.configs(configs);

        return newTopic;
    }

    /**
     * 通过<b>KafaKaAdminClient</b> 创建一个主题
     * <b>(一个分区数为4、副本因子为1的主题 topic-admin)</b>
     */
    public static void createTopicWithAdminClient() {
        // 通过initConfig()的配置来连接 Kafka 集群
        Properties props = initConfig();
        // 创建了一个 KafkaAdminClient 实例
        AdminClient client = AdminClient.create(props); // AdminClient.create() 方法实际上调用的就是 KafkaAdminClient 中的 createInternal 方法构建的 KafkaAdminClient 实例
        NewTopic newTopic = buildNewTopic();

        /**
         * 核心
         * KafkaAdminClient 内部使用 Kafka 的一套自定义二进制协议来实现诸如创建主题的管理功能。
         *  1.客户端根据方法的调用创建相应的协议请求，比如创建主题的 createTopics 方法，其内部就是发送 CreateTopicRequest 请求。
         *  2.客户端将请求发送至服务端。
         *  3.服务端处理相应的请求并返回响应，比如这个与 CreateTopicRequest 请求对应的就是 CreateTopicResponse。
         *  4.客户端接收相应的响应并进行解析处理。和协议相关的请求和相应的类基本都在 org.apache.kafka.common.requests 包下，AbstractRequest 和 AbstractResponse 是这些请求和响应类的两个基本父类。
         */
        CreateTopicsResult result = client.createTopics(Collections.singleton(newTopic)); // futures 的类型 Map<String, KafkaFuture> 中的 key 代表主题名称，而 KafkaFuture 代表创建后的返回值类型。
//        DeleteTopicsResult deleteTopics = client.deleteTopics(); // 与创建类型基本一致
//        DescribeTopicsResult describeTopics = client.describeTopics(Arrays.asList(TOPIC));
//        ListTopicsResult listResult = client.listTopics(); // 注意 CreateTopicsResult与ListTopicsResult类型不一致
        try {
            result.all().get(); // 未来 可能 将KafkaFuture 替代为 jdk8 CompletableFuture
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }




    public static void main(String[] args) {
        createTopicWithAdminClient();

    }
}
