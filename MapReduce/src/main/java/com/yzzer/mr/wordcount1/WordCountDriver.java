package com.yzzer.mr.wordcount1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class WordCountDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 声明配置对象
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9820");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("mapreduce.app-submission.cross-platform", "true");
        conf.set("yarn.resourcemanager.hostname", "slave01");

        // 声明job对象
        Job job = Job.getInstance(conf);

        // 指定驱动类
//        job.setJarByClass(WordCountDriver.class);
        job.setJar("/Users/yzzer/IdeaProjects/javas/OrientObjectPracice/MapReduce/target/MapReduce-1.0-SNAPSHOT.jar");
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

        // 提交Job
        job.waitForCompletion(true);
    }
}
