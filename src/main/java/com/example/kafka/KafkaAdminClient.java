package com.example.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
//import org.apache.kafka.test.TestUtils;
//import com.example.utils.testUtils;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author xyh
 * @date 2020/11/27 18:28
 */
public class KafkaAdminClient {

    private static final AclBinding ACL1 = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic3", PatternType.LITERAL),
            new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW));
    private static final AclBinding ACL2 = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic4", PatternType.LITERAL),
            new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.DESCRIBE, AclPermissionType.DENY));
    private static final AclBindingFilter FILTER1 = new AclBindingFilter(new ResourcePatternFilter(ResourceType.ANY, null, PatternType.LITERAL),
            new AccessControlEntryFilter("User:ANONYMOUS", null, AclOperation.ANY, AclPermissionType.ANY));
    private static final AclBindingFilter FILTER2 = new AclBindingFilter(new ResourcePatternFilter(ResourceType.ANY, null, PatternType.LITERAL),
            new AccessControlEntryFilter("User:bob", null, AclOperation.ANY, AclPermissionType.ANY));
    private static final AclBindingFilter UNKNOWN_FILTER = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.UNKNOWN, null, PatternType.LITERAL),
            new AccessControlEntryFilter("User:bob", null, AclOperation.ANY, AclPermissionType.ANY));

    /**
     * 创建kakfa B类集群AdminClient管理类
     * @return AdminClient
     */
    public static AdminClient adminClient() {
        Properties properties = new Properties();
//        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "55.11.59.173:9091,55.11.59.173:9092,55.11.59.173:9093");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.211.55.3:9092");

//        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
//        properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
//        StringBuilder sb = new StringBuilder();
//        sb.append("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"")
//                .append("admin").append("\" ").append("password").append("=\"").append("21da82c5-6e38-4c98-9ac4-913a1c6bd9c9").append("\";");
//        Password saslJaasConfig = new Password(sb.toString());
//        properties.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);

        return AdminClient.create(properties);
    }

    /**
     * 获取集群topic列表
     * @return Set<String>
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static Set<String> getTopicLists() throws ExecutionException,InterruptedException {
        AdminClient adminClient = adminClient();
        ListTopicsResult result = adminClient.listTopics();
        Set<String> topics = result.names().get();

        for (String topic : topics) {
            System.out.println(topic);
        }

        adminClient.close();
        return topics;
    }

    /**
     * 获取topic详细信息
     * @return Map<String, TopicDescription>
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static Map<String, TopicDescription> describeTopics(String topicName) throws ExecutionException,InterruptedException {
        AdminClient adminClient = adminClient();
        DescribeTopicsResult result = adminClient.describeTopics(Arrays.asList(topicName));
        Map<String, TopicDescription> descriptionMap = result.all().get();
        descriptionMap.forEach((key,value) ->
                System.out.println("name:" + key + ",desc" + value ));

        adminClient.close();
        return descriptionMap;
    }

    /**
     * 获取所有消费组id
     * @return Collection<ConsumerGroupListing>
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static Collection<ConsumerGroupListing> listConsumerGroups() throws ExecutionException,InterruptedException {
        AdminClient adminClient = adminClient();
        ListConsumerGroupsResult result = adminClient.listConsumerGroups();
        Collection<ConsumerGroupListing> consumerGroupListings = result.all().get();
        for (ConsumerGroupListing consumerGroupListing : consumerGroupListings) {
            System.out.println(consumerGroupListing.toString());
        }
        adminClient.close();
        return consumerGroupListings;
    }

    /**
     * 获取topic groupid的offset
     * @param groupId
     * @return Map<TopicPartition, OffsetAndMetadata>
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static Map<TopicPartition, OffsetAndMetadata> listConsumerGroupOffset(String groupId) throws ExecutionException,InterruptedException {
        AdminClient adminClient = adminClient();
        ListConsumerGroupOffsetsResult result = adminClient.listConsumerGroupOffsets(groupId);
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = result.partitionsToOffsetAndMetadata().get();

        for (TopicPartition topicPartition : topicPartitionOffsetAndMetadataMap.keySet()) {
            System.out.println("partition: " + topicPartition.partition() + "," + "offset: " + topicPartitionOffsetAndMetadataMap.get(topicPartition).offset());
        }
        adminClient.close();
        return topicPartitionOffsetAndMetadataMap;
    }

    /**
     * 更改topic配置
     * @param config
     * @return AlterConfigsResult
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static AlterConfigsResult alterConfigs(String config) throws ExecutionException,InterruptedException {
        AdminClient adminClient = adminClient();
        Map<ConfigResource, Config> configs = new HashMap<>();
//        configs.put();
        AlterConfigsResult alterConfigsResult = adminClient.alterConfigs(configs);

        adminClient.close();
        return alterConfigsResult;
    }

    /**
     * 获取配置信息
     * @param config
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static DescribeConfigsResult describeConfigs(String config) throws ExecutionException,InterruptedException {
        AdminClient adminClient = adminClient();
        List<ConfigResource> configs = new ArrayList<>();
        DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(configs);
        adminClient.close();
        return describeConfigsResult;
    }

    public static void describeACL(String config) throws ExecutionException,InterruptedException {
        AdminClient adminClient = adminClient();

    }

    /**
     * 创建topic
     * @param topicName
     * @param numPartition
     * @param day
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void createTopic(String topicName,Integer numPartition,Integer day) throws ExecutionException,InterruptedException {
        AdminClient adminClient = adminClient();

        NewTopic newTopic = new NewTopic(topicName,numPartition,(short) 1);
        Map<String, String> configs = new HashMap<>();
        String ms = String.valueOf(day * 3600 * 1000);
        configs.put(TopicConfig.RETENTION_MS_CONFIG,ms);
//        configs.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG,"");
        CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));
        try {
            //测试创建成功无法获得返回值，后面在公司电脑上验证下
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        System.out.println(result.all().get()); //KafkaFuture{value=null,exception=null,done=true}
        adminClient.close();

    }

    public static void createACL() throws ExecutionException,InterruptedException {
        AdminClient adminClient = adminClient();

        CreateAclsResult result = adminClient.createAcls(Arrays.asList(ACL1));

//        result.all().get()


    }



    public static void main(String[] args) throws Exception{
//        getTopicLists();
//        describeTopics("test");
//        listConsumerGroups();
        listConsumerGroupOffset("test");
//        createTopic("test999",3,1);
        //alterConfigs();


    }

}
