package com.yzzer.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * kafka消费者 offset提交问题
 *  1。自动提交
 *      enable.auto.commit = true
 *      auto.commit.interval.ms = 1000
 *  2. 手动提交
 *      enable.auto.commit = false
 *      1) 同步提交
 *      2) 异步提交
 */
public class KafkaConsumerDemo3 {
    public static void main(String[] args) {
        // 获取配置
        Properties props = new Properties();

        // kafka集群位置
        props.put("bootstrap.servers", "hadoop105:9092");

        // 消费者组id
        props.put("group.id", "yzzer2");

        // 自动提交offset
        props.put("enable.auto.commit", "false");

        // 重置offset  如果有记录就接着上次的消费
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

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
            System.out.println("下一次消费");
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : poll) {
                System.out.println("消费到：" + record.topic() +
                        ":" + record.partition()
                + ":" + record.offset()
                + ":" + record.key()
                + ":" + record.value());
            }
//            consumer.commitSync(); //同步提交
//            System.out.println("同步提交");
//            consumer.commitAsync(new OffsetCommitCallback() {
//                // 当offset提交后回调
//                @Override
//                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
//                    if(exception==null){
//                        System.out.println("提交成功"+offsets);
//                    }else{
//                        System.out.println("提交失败");
//                    }
//                }
//            });  // 异步提交
        }
        // 关闭对象
//        consumer.close();
    }

}
