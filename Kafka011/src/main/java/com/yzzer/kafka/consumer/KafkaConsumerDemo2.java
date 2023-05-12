package com.yzzer.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * kafka消费者 offset重置问题
 *
 *      auto.offset.reset = earliest/latest
 *      重置的情况：
 *          1。 新的组吗，没有消费记录
 *          2。 要消费的offset对应的消息已经被删除
 */
public class KafkaConsumerDemo2 {
    public static void main(String[] args) {
        // 获取配置
        Properties props = new Properties();

        // kafka集群位置
        props.put("bootstrap.servers", "hadoop102:9092, hadoop103:9092, hadoop104:9092, hadoop105:9092, hadoop106:9092");

        // 消费者组id
        props.put("group.id", "yzzer3");

        // 自动提交offset
        props.put("enable.auto.commit", "true");

        // 重置offset  如果有记录就接着上次的消费
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

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
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofSeconds(2));
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
