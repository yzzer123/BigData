package com.yzzer.apitest.source;

import org.apache.calcite.util.Sources;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileSource {

    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("hadoop202", 8081,
//                "/Users/yzzer/IdeaProjects/javas/BigData/Flink/target/Flink-1.0.0.jar");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> streamSource = env.readTextFile("/Users/yzzer/IdeaProjects/javas/BigData/Flink/src/main/resources/sensor.txt");

        streamSource.print();

        env.execute("FileSource");
    }
}
