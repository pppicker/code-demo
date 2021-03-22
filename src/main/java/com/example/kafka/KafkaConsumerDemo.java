package com.example.kafka;

import com.example.utils.DateUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * @author xyh
 * @date 2020/11/21 23:13
 */
public class KafkaConsumerDemo {

    private static KafkaConsumer<String,String> consumer;
    static{
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
    }

    //获取CONSUMER的OFFSET
    public static void getGroupOffset(){

        //获取下一个offset
        ConsumerRecords<String,String> records = consumer.poll(100);
        for (ConsumerRecord record : records) {
            System.out.println(record.offset());
        }


//        System.out.println("开始尝试拉取消息");
//        consumer.poll(100);
//        Set<TopicPartition> tps = consumer.assignment();
//        List<PartitionInfo> pif = consumer.partitionsFor("final-test");
//        for (TopicPartition tp : tps) {
//            Long offset = consumer.position(tp);
//            System.out.println("partition:" + tp.partition() + ", " + "offset:" + offset);
//        }

    }

    public static void modifyGroupOffsetByDateStr(String dateStr) {
        consumer.poll(100L);
        Set<TopicPartition> assingment = consumer.assignment();

        Map<TopicPartition, Long> map = new HashMap<>();
        for (TopicPartition tp : assingment) {
            map.put(tp, DateUtil.strToTimestamp(dateStr));
        }
        Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(map);
        for (TopicPartition topicPartition : offsets.keySet()) {
            OffsetAndTimestamp offsetAndTimestamp = offsets.get(topicPartition);
            if(offsetAndTimestamp != null) {
                consumer.seek(topicPartition,offsetAndTimestamp.offset());
            }
        }
        consumer.close();



    }

    public static void main(String[] args) {

        String dateStr = "";

        getGroupOffset();

        modifyGroupOffsetByDateStr(dateStr);

        //开始消费
//        while (true) {
//            ConsumerRecords<String,String> records = consumer.poll(100);
//            for (ConsumerRecord record : records) {
//                System.out.println(record.value());
//            }
//            consumer.commitSync();
//        }

    }


}
