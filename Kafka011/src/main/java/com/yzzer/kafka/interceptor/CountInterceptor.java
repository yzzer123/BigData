package com.yzzer.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 统计发送失败和成功的消息个数
 *
 */
public class CountInterceptor implements ProducerInterceptor<String, String> {
    private int success;
    private int fail;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if(exception != null){
            fail++;
        }else{
            success++;
        }
    }

    @Override
    public void close() {
        System.out.println("SUCCESS:" + success);
        System.out.println("FAIL:" + fail);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
