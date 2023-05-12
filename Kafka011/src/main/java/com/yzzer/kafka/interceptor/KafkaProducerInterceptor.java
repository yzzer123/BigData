package com.yzzer.kafka.interceptor;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

/**
 * 生产者 异步发送 不带回调
 *
 * 配置类 ：
 *      ProducerConfig
 *      CommonClientConfigs
 *      ConsumerConfig
 *
 */

public class KafkaProducerInterceptor {

    public static void main(String[] args) {
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


        // 配置拦截器
        ArrayList<String> interceptors = new ArrayList<>();
        interceptors.add("com.yzzer.kafka.interceptor.TimestampInterceptor");
        interceptors.add("com.yzzer.kafka.interceptor.CountInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);


        // 1. 创建生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);



        // 2. 生产数据

        for (int i = 0; i < 20; i++) {

            kafkaProducer.send(new ProducerRecord<String, String>("test","yzzeryzzeryzzeryzzer" + i));

        }

        // 3. 关闭对象
        kafkaProducer.close();

    }


}
