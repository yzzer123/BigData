package com.yzzer.kafka.producer;


import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 生产者 自定义分区
 *
 * 配置类 ：
 *      ProducerConfig
 *      CommonClientConfigs
 *      ConsumerConfig
 *
 */

public class KafkaProducerPartition {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 0. 创建配置对象
        Properties props = new Properties();
        //kafka集群，broker-list
        props.put("bootstrap.servers", "hadoop102:9092");

        props.put("acks", "all");

        //重试次数
        props.put("retries", 3);

        //批次大小
        props.put("batch.size", 16384);

        //等待时间
        props.put("linger.ms", 1);

        //RecordAccumulator缓冲区大小
        props.put("buffer.memory", 33554432); // 32M mapreduce本地模式块大小为32M

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 设置分区
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.yzzer.kafka.partitioner.MyPartitioner");

        // 1. 创建生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        // 2. 生产数据
        for (int i = 0; ; i++) {

            String str = Math.random() > 0.5 ? "yzzer" : "agewrg#@&" + Math.random();

            kafkaProducer.send(new ProducerRecord<String, String>("test", str + i),
                    new Callback() {
                        /**
                         * 当消息发送完成后，会调用该方法
                         * @param metadata  消息的元数据信息-消息发到哪里
                         * @param exception   消息发送过程如果抛出异常会传入该方法
                         */
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception != null) {
                                System.out.println("消息发送失败：" + exception.getMessage());
                            } else {
                                System.out.println("消息发送成功: " + metadata.topic() + ":"
                                        + metadata.partition() + ":" + metadata.offset());
                            }
                        }
                    });

        }

        // 3. 关闭对象
//        kafkaProducer.close();

    }


}
