package com.yzzer.mr.writable;

import com.yzzer.mr.mywordcount.MyPartionner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCountDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(FlowCountDriver.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        job.setCombinerClass(FlowReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(FlowBean.class);
        job.setPartitionerClass(MyPartionner.class);
        job.setNumReduceTasks(6);
        FileInputFormat.setInputPaths(job, new Path("/Volumes/yzzerport/hadoop/02.资料/07_测试数据/phone_data/phone_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/yzzer/lib/flowcount"));

        job.waitForCompletion(true);
    }

    public static class FlowMapper extends Mapper<Object, Text, Text, FlowBean>{

        private FlowBean outv = new FlowBean();
        private Text outk = new Text();
        /**
         * 提取每一行的数据，并进行分词
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
            // 获取当前行数据
            String[] items = value.toString().split("\t");

            // 获取数据
            // 1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
            outk.set(items[1]);  // 手机号

            outv.setUpFlow(Integer.parseInt(items[items.length-3]));
            outv.setDownFlow(Integer.parseInt(items[items.length-2]));
            outv.setSumFlow();
            context.write(outk, outv);
        }
    }

    public static class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
//        private Text outk = new Text();
        private FlowBean outv = new FlowBean();

        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
            // 遍历values
//            outk.set(key);
            int totalUpFlow = 0;
            int totalDownFlow = 0;

            for(FlowBean value: values){
                totalUpFlow += value.getUpFlow();
                totalDownFlow += value.getDownFlow();
            }

            outv.setUpFlow(totalUpFlow);
            outv.setDownFlow(totalDownFlow);
            outv.setSumFlow();
            context.write(key, outv);
        }
    }

}
