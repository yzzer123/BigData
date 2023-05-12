package com.yzzer.mr.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9820");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("mapreduce.app-submission.cross-platform", "true");
        conf.set("yarn.resourcemanager.hostname", "slave01");

        Job job = Job.getInstance(conf);

//        job.setJarByClass(WordCountDriver.class);
        job.setJar("/Users/yzzer/IdeaProjects/javas/OrientObjectPracice/MapReduce/target/MapReduce-1.0-SNAPSHOT.jar");

        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);  // 设置合适的combiner会提高MR的效率
        job.setReducerClass(WordCountReducer.class);

//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);
        job.setInputFormatClass(CombineTextInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("hdfs://master:9820/wcinput"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9820/wcoutput4"));

        job.waitForCompletion(true);
    }

     static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable>{
        private Text outk = new Text();
        private IntWritable outv = new IntWritable(1);
//
//        @Override
//        protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
//            System.out.println("mapper begin");
//        }

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            while(itr.hasMoreTokens()) {
                this.outk.set(itr.nextToken());
                context.write(this.outk, outv);
            }
        }

//        @Override
//        protected void cleanup(Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
//            System.out.println("mapper finished");
//        }
    }

     static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        private IntWritable res = new IntWritable();

//        @Override
//        protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
//            System.out.println("reducer/combiner begin");
//        }

         @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            IntWritable val;
            for(Iterator<IntWritable> it = values.iterator(); it.hasNext(); sum += val.get()){
                val = it.next();
            }
            res.set(sum);
            context.write(key, res);
        }

//        @Override
//        protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
//            System.out.println("reducer/combiner finished");
//        }
    }
}
