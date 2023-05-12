package com.yzzer.apitest.transformer;

import com.yzzer.apitest.source.DataSource.MySource;
import com.yzzer.apitest.source.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class TransformerSplit {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<SensorReading> streamSource = env.addSource(new MySource());


        // 侧输出流
        OutputTag<SensorReading> sensor_1_5 = new OutputTag<SensorReading>("sensor_1"){};


        // process
        SingleOutputStreamOperator<SensorReading> processedStream = streamSource.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, ProcessFunction<SensorReading, SensorReading>.Context ctx, Collector<SensorReading> out) throws Exception {
                if (Integer.parseInt(value.getId().substring(7)) <= 5) {
                    ctx.output(sensor_1_5, value);  // 侧输出流收集
                } else {
                    out.collect(value);
                }
            }
        });

        // 侧输出流获取
        DataStream<SensorReading> sideOutput = processedStream.getSideOutput(sensor_1_5);
//        sideOutput.print("sideOutput");
//        processedStream.print("mainStream");

        ConnectedStreams<SensorReading, SensorReading> connectedStreams = processedStream.connect(sideOutput);
        SingleOutputStreamOperator<SensorReading> connectedMappedStream = connectedStreams.map(new CoMapFunction<SensorReading, SensorReading, SensorReading>() {
            @Override
            public SensorReading map1(SensorReading value) throws Exception {
                return value;
            }

            @Override
            public SensorReading map2(SensorReading value) throws Exception {
                return value;
            }
        });

        connectedMappedStream.print("connected");

        env.execute();
    }

}
