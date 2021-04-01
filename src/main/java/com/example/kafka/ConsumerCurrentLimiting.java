package com.example.kafka;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author xyh
 * @date 2021/3/26 10:29
 */
public class ConsumerCurrentLimiting {
    /*** 令牌生成速率，单位为秒 */
    public static final int permitsPerSecond = 1;
    /*** 限流器 */
    private static final RateLimiter LIMITER = RateLimiter.create(permitsPerSecond);

    /**
     * 创建Consumer实例
     */
    public static Consumer<String, String> createConsumer() {

        KafkaConsumer<String,String> consumer;
        //设置kafka启动需要的参数
        Properties properties = new Properties();
        //（kafka集群地址）broker地址
        properties.put("bootstrap.servers","10.211.55.3:9092");
        //key的序列化方式
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        //value的序列化方式
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
//        properties.put("auto.offset.reset", "earliest");
        properties.put("group.id","test");
        properties.put("enable.auto.commit", "false");
        properties.put("max.poll.RECORDS", 3);
        consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("final-test"));
        return consumer;
    }

    /**
     * 演示对Consumer限流
     */
    public static void currentLimiting() {
        Consumer<String, String> consumer = createConsumer();
        TopicPartition p0 = new TopicPartition("MyTopic", 0);
        TopicPartition p1 = new TopicPartition("MyTopic", 1);
//        consumer.assign(List.of(p0, p1));
        consumer.assign(Arrays.asList(p0, p1));

        while (true) {
            // 从Topic中拉取数据，每100毫秒拉取一次
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1));
            if (records.isEmpty()) {
                continue;
            }

            // 限流
            if (!LIMITER.tryAcquire()) {
                System.out.println("无法获取到令牌，暂停消费");
                consumer.pause(Arrays.asList(p0, p1));
            } else {
                System.out.println("获取到令牌，恢复消费");
                consumer.resume(Arrays.asList(p0, p1));
            }

            // 单独处理每一个Partition中的数据
            for (TopicPartition partition : records.partitions()) {
                System.out.println("======partition: " + partition + " start======");
                // 从Partition中取出数据
                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                for (ConsumerRecord<String, String> record : partitionRecords) {
                    try {
                        // 模拟将数据写入数据库
                        Thread.sleep(1000);
                        System.out.println("save to db...");
                        System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n",
                                record.partition(), record.offset(), record.key(), record.value());
                    } catch (Exception e) {
                        // 发生异常直接结束，不提交offset
                        e.printStackTrace();
                        return;
                    }
                }

                // 执行成功则取出当前消费到的offset
                long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                // 由于下一次开始消费的位置是最后一次offset+1的位置，所以这里要+1
                OffsetAndMetadata metadata = new OffsetAndMetadata(lastOffset + 1);
                // 针对Partition提交offset
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                offsets.put(partition, metadata);
                // 同步提交offset
                consumer.commitSync(offsets);
                System.out.println("======partition: " + partition + " end======");
            }
        }
    }

    public static void main(String[] args) {
        currentLimiting();
    }
}