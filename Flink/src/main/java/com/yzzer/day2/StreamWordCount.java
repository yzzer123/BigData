package com.yzzer.day2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置远程提交
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 8081,
                "/Users/yzzer/IdeaProjects/javas/BigData/Flink/target/Flink-1.0.0.jar");

        // 设置并行度
//        env.setParallelism(1);

//        ParameterTool tool = ParameterTool.fromArgs(args);

//        String host = tool.get("host");
//        int port = tool.getInt("port");
        String host = "localhost";
        int port = 7777;

        // 获取数据源  提交数据源 nc -lk 9876
        DataStreamSource<String> streamSource = env.socketTextStream(host, port);

        // 对文本进行切分  hello flink -> <hello, 1> <flink, 1>
        SingleOutputStreamOperator<Tuple2<String, Long>> mappedStream = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1L));
                }
            }
        });
//                .setParallelism(2).slotSharingGroup("red"); // slot共享组

        // 对mappedStream去分组合并
//        SingleOutputStreamOperator<Tuple2<String, Long>> result = mappedStream.keyBy(0).sum(1);

//        SingleOutputStreamOperator<Tuple2<String, Long>> result =
//
//        DataStream<Tuple2<String, Long>> result =
//                            mappedStream.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
//                                @Override
//                                public String getKey(Tuple2<String, Long> value) throws Exception {
//                                    return value.f0;
//                                }
//                            })
//                            .reduce(new ReduceFunction<Tuple2<String, Long>>() {
//                                @Override
//                                public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
//                                    if (value1 == null) {
//                                        return value2;
//                                    }
//                                    value1.setField(value1.f1 + value2.f1, 1);
//                                    return value1;
//                                }
//                            })
//            //                        .setParallelism(2)
//            //                        .disableChaining()
//            //                        env.disableOperatorChaining();  // 设置不one to one 合并
////                            .shuffle();
//                            .rebalance()
////                          .slotSharingGroup("blue");
//                            ;

//        result.print();
        mappedStream.print();
        env.execute("word count");

    }

}
