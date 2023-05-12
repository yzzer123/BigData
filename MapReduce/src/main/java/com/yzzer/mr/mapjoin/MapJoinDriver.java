package com.yzzer.mr.mapjoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * 处理数据倾斜的问题，没有Reduce端
 */
public class MapJoinDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(MapJoinDriver.class);
        job.setMapperClass(ReducerJoinMapper.class);
//        job.setReducerClass(ReducerJoinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);
        job.addCacheFile(new URI("file:///Users/yzzer/Documents/技术文档/hadoop/07_测试数据/reducejoin/pd.txt"));
//        job.setOutputFormatClass(LogFormat.class);
        FileInputFormat.setInputPaths(job, new Path("/Users/yzzer/Documents/技术文档/hadoop/07_测试数据/mapjoin"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/yzzer/lib/mapjoinoutput"));

        job.waitForCompletion(true);
    }

    static class ReducerJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private Text outk = new Text();
        private HashMap<String, String> pd = new HashMap<>();


        @Override
        protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream inputStream = fs.open(new Path(cacheFiles[0]));
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            // 1 小米
            String line;
            while((line = reader.readLine()) != null){
                String[] items = line.split("\t");
                pd.put(items[0], items[1]);
            }
            reader.close();
            IOUtils.closeStream(inputStream);
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split("\t");
            outk.set(items[0]+"\t"+pd.get(items[1])+"\t"+items[2]);
            context.write(outk, NullWritable.get());
        }
    }



}
