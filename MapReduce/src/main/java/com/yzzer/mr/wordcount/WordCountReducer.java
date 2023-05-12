package com.yzzer.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * wordcount为例
 * 自定义Reducer类 需要继承Hadoop提供的Reducer，并且根据业务需求指定输入数据和输出数据类型
 * 输入数据类型
 * KEYIN  Map输出的key类型 word Text
 * VALUEIN Map输出的value类型  标记 IntWritable
 * 输出数据类型
 * KEYOUT   输出数据KEY类型  就是一个单词 Text
 * VALUEOUT 输出数据的Value类型 单词计数 IntWritable
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private Text outk = new Text();
    private IntWritable outv = new IntWritable(1);
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int total = 0;
        for(IntWritable value: values){
            total += value.get();
        }
        outk.set(key);
        outv.set(total);
        context.write(outk, outv);
    }
}
