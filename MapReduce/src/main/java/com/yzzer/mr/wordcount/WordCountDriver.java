package com.yzzer.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;


public class WordCountDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {


        // 声明配置对象
        Configuration conf = new Configuration();
        // 设置在mapper端输出的时候进行压缩
//        conf.setBoolean("mapreduce.map.output.compress", true);
        // 默认解码器
//        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.DefaultCodec");
        // reduce 输出压缩
//        conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
//        conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.DefaultCodec");


        // 声明job对象
        Job job = Job.getInstance(conf);

        // 指定驱动类
        job.setJarByClass(WordCountDriver.class);
        // 指定当前job的Mapper
        job.setMapperClass(WordCountMapper.class);
        // 指定reducer
        job.setReducerClass(WordCountReducer.class);
        // 指定Map端输出数据的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 指定最终输出结果的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 指定输入数据的目录和输出数据的目录
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.setInputPaths(job, new Path("/Users/yzzer/Documents/技术文档/hadoop/07_测试数据/jianai"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/yzzer/lib/reducezipoutput"));

        // 提交Job
        job.waitForCompletion(true);
    }
}
