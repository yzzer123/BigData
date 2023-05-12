package com.yzzer.mr.writablecomparable;

import com.yzzer.mr.mywordcount.MyPartionner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
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
//        job.setCombinerClass(FlowReducer.class);
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(FlowBean.class);
        job.setSortComparatorClass(FlowBeanComparator.class);  // 设置比较器对象
//        job.setPartitionerClass(MyPartionner.class);
//        job.setNumReduceTasks(6);
        FileInputFormat.setInputPaths(job, new Path("/Volumes/yzzerport/hadoop/02.资料/07_测试数据/phone_data/phone_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/yzzer/lib/flowsort"));

        job.waitForCompletion(true);
    }

    public static class FlowMapper extends Mapper<Object, Text, FlowBean, Text>{

        private FlowBean outk = new FlowBean();
        private Text outv = new Text();
        /**
         * 提取每一行的数据，并进行分词
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, FlowBean, Text>.Context context) throws IOException, InterruptedException {
            // 获取当前行数据
            String[] items = value.toString().split("\t");

            // 获取数据
            // 1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
            outv.set(items[1]);  // 手机号

            outk.setUpFlow(Integer.parseInt(items[items.length-3]));
            outk.setDownFlow(Integer.parseInt(items[items.length-2]));
            outk.setSumFlow();
            context.write(outk, outv);
        }
    }

    public static class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean>{

        @Override
        protected void reduce(FlowBean key, Iterable<Text> values, Reducer<FlowBean, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {

            for (Text value : values) {
                context.write(value, key);
            }
        }
    }

}
