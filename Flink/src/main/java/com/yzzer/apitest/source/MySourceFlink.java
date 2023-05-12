package com.yzzer.apitest.source;

import com.yzzer.apitest.source.DataSource.MySource;
import com.yzzer.apitest.source.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MySourceFlink {

    public static void main(String[] args) throws Exception {

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("hadoop202", 8081,
                "/Users/yzzer/IdeaProjects/javas/BigData/Flink/target/Flink-1.0.0.jar");

        DataStreamSource<SensorReading> streamSource = env.addSource(new MySource());

        streamSource.print();

        env.execute("MySourceFlink");
    }
}
