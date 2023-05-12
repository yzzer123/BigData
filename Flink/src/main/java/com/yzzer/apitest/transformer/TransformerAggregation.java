package com.yzzer.apitest.transformer;

import com.sun.org.apache.xerces.internal.impl.xpath.regex.REUtil;
import com.yzzer.apitest.source.DataSource.MySource;
import com.yzzer.apitest.source.MySourceFlink;
import com.yzzer.apitest.source.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import scala.collection.mutable.ReusableBuilder;

import java.util.Objects;

public class TransformerAggregation {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

//        DataStreamSource<String> streamSource = env.readTextFile("/Users/yzzer/IdeaProjects/javas/BigData/Flink/src/main/resources/sensor.txt");
        DataStreamSource<SensorReading> streamSource = env.addSource(new MySource());


        // 转换为sensorReading 类型
//        SingleOutputStreamOperator<SensorReading> mapStream = streamSource.map(line -> {
//            String[] items = line.split(",");
//            return new SensorReading(items[0], Long.parseLong(items[1]), Double.parseDouble(items[2]));
//        });
        SingleOutputStreamOperator<SensorReading> mapStream = streamSource.map(item->item);



        // 分组
        KeyedStream<SensorReading, String> keyedStream = mapStream.keyBy(new KeySelector<SensorReading, String>() {

            @Override
            public String getKey(SensorReading value) throws Exception {
                return value.getId();
            }
        });
//        KeyedStream<SensorReading, String> keyedStream = mapStream.keyBy(SensorReading::getId);
//        KeyedStream<SensorReading, Tuple> keyedStream = mapStream.keyBy("id");

//        SingleOutputStreamOperator<SensorReading> resultStream = keyedStream.max("temperature"); // 只会更新该域
//        SingleOutputStreamOperator<SensorReading> resultStream = keyedStream.maxBy("temperature"); // 会把其他字段换成最大的对象


//        SingleOutputStreamOperator<SensorReading> resultStream = keyedStream.reduce(new ReduceFunction<SensorReading>() {
//            @Override
//            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
//                return new SensorReading(value2.getId(), value2.getTimestamp(),
//                        Math.max(value1.getTemperature(), value2.getTemperature()));
//            }
//        });

        SingleOutputStreamOperator<SensorReading> resultStream = keyedStream.reduce((curState, newData) ->
                new SensorReading(curState.getId(), newData.getTimestamp(),
                    Math.max(curState.getTemperature(), newData.getTemperature()))
        );




//        DataStream<SensorReading> sideOutput = resultStream.getSideOutput(new OutputTag<SensorReading>("dirty") {
//
//        });

        resultStream.print();


        env.execute();
    }
}
