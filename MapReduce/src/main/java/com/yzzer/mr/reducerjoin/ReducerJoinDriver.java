package com.yzzer.mr.reducerjoin;

import com.yzzer.mr.outputformat.LogFormat;
import com.yzzer.mr.outputformat.LogMRDriver;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class ReducerJoinDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(ReducerJoinDriver.class);
        job.setMapperClass(ReducerJoinMapper.class);
        job.setReducerClass(ReducerJoinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinBean.class);
        job.setOutputKeyClass(JoinBean.class);
        job.setOutputValueClass(NullWritable.class);

//        job.setOutputFormatClass(LogFormat.class);
        FileInputFormat.setInputPaths(job, new Path("/Users/yzzer/Documents/技术文档/hadoop/02.资料/07_测试数据/reducejoin"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/yzzer/lib/joinoutput"));

        job.waitForCompletion(true);
    }

    static class ReducerJoinMapper extends Mapper<LongWritable, Text, Text, JoinBean> {
        private Text outk = new Text();
        private JoinBean outv = new JoinBean();
        private FileSplit inputSplit;


        @Override
        protected void setup(Mapper<LongWritable, Text, Text, JoinBean>.Context context) throws IOException, InterruptedException {
            inputSplit = (FileSplit) context.getInputSplit();
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, JoinBean>.Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split("\t");
            // 判断表类型
            if(inputSplit.getPath().getName().contains("order")){
                //1001	01	1
                outv.setOrigin("order");
                outv.setOrderId(items[0]);
                outv.setPid(items[1]);
                outv.setMount(Integer.parseInt(items[2]));
                outv.setPname("");
                outk.set(items[1]);
            }else{
                //01	小米
                outv.setOrigin("pd");
                outv.setOrderId("");
                outv.setPid(items[0]);
                outv.setMount(0);
                outv.setPname(items[1]);
                outk.set(items[0]);
            }
            context.write(outk, outv);
        }
    }


    static class ReducerJoinReducer extends Reducer<Text, JoinBean, JoinBean, NullWritable> {
        private ArrayList<JoinBean> orderList = new ArrayList<>();
        private JoinBean pd = new JoinBean();

        @Override
        protected void reduce(Text key, Iterable<JoinBean> values, Reducer<Text, JoinBean, JoinBean, NullWritable>.Context context) throws IOException, InterruptedException {
            for (JoinBean value : values) {
                if(value.getOrigin().contains("order")){
                    try {
                        JoinBean newBean = new JoinBean();
                        BeanUtils.copyProperties(newBean, value);
                        orderList.add(newBean);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }else{
                    try {
                        BeanUtils.copyProperties(pd, value);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }
            }

            for (JoinBean item : orderList) {
                item.setPname(pd.getPname());
                context.write(item, NullWritable.get());
            }
            orderList.clear();
        }
    }
}
