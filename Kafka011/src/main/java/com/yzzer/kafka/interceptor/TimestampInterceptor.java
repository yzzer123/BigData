package com.yzzer.kafka.interceptor;


import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 实现将时间戳添加到消息的前面
 */
public class TimestampInterceptor implements ProducerInterceptor<String, String> {
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // 获取value
        String value = record.value();

        // 添加时间戳
        String result = System.currentTimeMillis() + "->" + value;

        // 构建新的消息对象
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key()
                , result);
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
