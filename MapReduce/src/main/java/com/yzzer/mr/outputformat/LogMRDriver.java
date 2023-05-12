package com.yzzer.mr.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogMRDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(LogMRDriver.class);
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(LogFormat.class);
        FileInputFormat.setInputPaths(job, new Path("/Users/yzzer/Documents/技术文档/hadoop/02.资料/07_测试数据/log/"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/yzzer/lib/logoutput"));

        job.waitForCompletion(true);

    }

    static class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            context.write(value, NullWritable.get());
        }
    }

    static class LogReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            for (NullWritable value : values) {
                context.write(key, NullWritable.get());
            }
        }
    }
}
