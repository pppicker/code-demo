package com.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class KafkaProducerDemo {
    private static KafkaProducer<String,String> producer;
    static{
        //设置kafka启动需要的参数
        Properties properties = new Properties();
        //（kafka集群地址）broker地址
        properties.put("bootstrap.servers","10.211.55.3:9092");
        //key的序列化方式
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //value的序列化方式
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(properties);
    }
    //发送消息不关注返回结果
    public static void sendMessage(){

        for (int i = 0; i < 20 ; i++) {
            //ProducerRecord的三个参数，topic，发送的key，发送的value
            ProducerRecord<String,String> record =
                    new ProducerRecord<>("final-test","key-" + i,"message-" + i);
            producer.send(record);
            System.out.println("key: " + "key-"+i + ", " + "value: " + "message-"+i);
        }
        //关闭producer
        producer.close();
    }

    public static void sendMessageCallBack(){
        ProducerRecord<String,String> record =
                new ProducerRecord<>("user-info-topic","name","路飞");
        producer.send(record,new MyCallBack());
        producer.close();
    }

    private static class MyCallBack implements Callback {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            System.out.println("进入异步发送回调函数");
            if(exception != null){
                System.out.println("出现异常："+exception.getMessage());
            }
            System.out.println(String.format("发送结果：topic:%s,存储的partition:%s，offset:%s",
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset()));
        }
    }


    public static void main(String[] args) {
        sendMessage();
    }
}
