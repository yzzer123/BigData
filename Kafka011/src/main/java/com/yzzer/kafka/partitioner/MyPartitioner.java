package com.yzzer.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;

public class MyPartitioner implements Partitioner {
    /**
     * 计算分区号
     * 包含yzzer发送 0 号分区
     * 之外发送到其他分区
     * @param topic
     * @param key
     * @param keyBytes
     * @param value
     * @param valueBytes
     * @param cluster
     * @return
     */
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if(value.toString().contains("yzzer")){
            return 0;
        }else{
            int numPartitions = cluster.partitionCountForTopic(topic);
            return (int) (Math.round(Math.random()*1000) % (numPartitions-1) + 1);
        }
    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}
