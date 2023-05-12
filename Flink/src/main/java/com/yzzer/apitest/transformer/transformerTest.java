package com.yzzer.apitest.transformer;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class transformerTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env
                .readTextFile("/Users/yzzer/IdeaProjects/javas/BigData/Flink/src/main/resources/sensor.txt");


        // map算子  String -> Integer
        SingleOutputStreamOperator<Integer> mapStream = streamSource.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return value.length();
            }
        });

        // flatmap  String -> String[]

        SingleOutputStreamOperator<String> flatMapStream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(",");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        // filer 筛选 sensor_1
        SingleOutputStreamOperator<String> sensorStream = streamSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.contains("sensor_1");
            }
        });

        // 打印
        mapStream.print("map");
        flatMapStream.print("flatmap");
        sensorStream.print("filter");

        env.execute();
    }
}
