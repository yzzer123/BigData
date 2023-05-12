package com.yzzer.day1;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
//import
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
// 从socket获取数据，然后处理
// word count
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 获取运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行任务数量
//        env.setParallelism(1);

        // 获取数据源

        DataStreamSource<String> streamSource = env.readTextFile("/Users/yzzer/IdeaProjects/javas/BigData/Flink/src/main/resources/text.txt");

        // map操作 flatmap是输入一个元素，输出一个或多个元素
        SingleOutputStreamOperator<WordWithCount> mappedStream = streamSource.
                // 指定输入泛型为String， 输出泛型为WordWithCount
                        flatMap(new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                        String[] arr = value.split(" ");
                        for (String s : arr) {
                            out.collect(new WordWithCount(s, 1L));
                        }
                    }
                });

        // 分组
        KeyedStream<WordWithCount, String> keyedStream = mappedStream
                // 输入为WordWithCount，输出为分组键
                .keyBy(new KeySelector<WordWithCount, String>() {
                    @Override
                    public String getKey(WordWithCount value) throws Exception {
                        return value.word;
                    }
                });

        // 聚合 reduce操作
        SingleOutputStreamOperator<WordWithCount> result = keyedStream.reduce(new ReduceFunction<WordWithCount>() {
            @Override
            public WordWithCount reduce(WordWithCount value1, WordWithCount value2) throws Exception {
                return new WordWithCount(value1.word, value1.count + value2.count);
            }
        });

        //  数据下沉或打印
        result.print();

        // 惰性提交，执行程序
        env.execute();

    }

    // POJO类
    /*
    1.必须是公有类
    2.所有字段public
    3.必须有空构造器
    4.toString Override
     */

    public static class WordWithCount {
        public String word;
        public Long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, Long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
