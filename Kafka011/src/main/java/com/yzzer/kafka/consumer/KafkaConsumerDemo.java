package com.yzzer.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.internals.Topic;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * kafka消费者
 */
public class KafkaConsumerDemo {
    public static void main(String[] args) {
        // 获取配置
        Properties props = new Properties();

        // kafka集群位置
        props.put("bootstrap.servers", "hadoop102:9092");

        // 消费者组id
        props.put("group.id", "yzzer2");

        // 自动提交offset
        props.put("enable.auto.commit", "true");

        props.put("auto.commit.interval.ms", "1000");

        // 反序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        // 创建消费对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // 订阅主题(可以多个)
        List<String> topics = new ArrayList<String>();
        topics.add("test");
        consumer.subscribe(topics);

        // 持续消费数据

        while (true){
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(1));
            for (ConsumerRecord<String, String> record : poll) {
                System.out.println("消费到：" + record.topic() +
                        ":" + record.partition()
                + ":" + record.offset()
                + ":" + record.key()
                + ":" + record.value());
            }
        }
        // 关闭对象
//        consumer.close();
    }

}
