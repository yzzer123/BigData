package com.yzzer.apitest.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class KafkaSourceFlink {
    public static void main(String[] args) throws Exception{
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("hadoop202", 8081,
                "/Users/yzzer/IdeaProjects/javas/BigData/Flink/target/Flink-1.0.0.jar");
//         见 https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/datastream/kafka/
//
//         从kafka获取数据
//        env.setParallelism(8);
//        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
//                .setBootstrapServers("hadoop201:9092,hadoop202:9092,hadoop203:9092")
//                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
//                .setGroupId("yzzer")
//                .setTopics("ods_base_log")
//                .build();
//
//        DataStreamSource<String> streamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka source");


        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop201:9092,hadoop202:9092,hadoop203:9092");
        props.setProperty("group.id", "yzzer");
        props.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>("ods_base_log", new SimpleStringSchema(), props);
        consumer.setStartFromEarliest();

        DataStreamSource<String> streamSource = env.addSource(consumer, "kafka source");

        streamSource.print(streamSource.getParallelism()+"paral");
        env.execute("kafka source");

    }
}
