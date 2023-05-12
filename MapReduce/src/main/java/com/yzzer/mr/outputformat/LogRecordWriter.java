package com.yzzer.mr.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Hadoop 的 RecorderWriter
 */

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {
    private final FileSystem fs;
    private final String sinaPath = "/Users/yzzer/lib/sina.txt";
    private final String otherPath = "/Users/yzzer/lib/other.txt";
    private final FSDataOutputStream sinaStream;
    private final FSDataOutputStream otherStream;


    public LogRecordWriter(TaskAttemptContext job) throws IOException {
        fs = FileSystem.get(job.getConfiguration());
        sinaStream = fs.create(new Path(sinaPath), true);
        otherStream = fs.create(new Path(otherPath), true);
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String keyStr = key.toString();
        if(keyStr.contains("sin")){
            sinaStream.writeBytes(keyStr+"\n");
        }else{
            otherStream.writeBytes(keyStr+"\n");
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(sinaStream);
        IOUtils.closeStream(otherStream);
        fs.close();
    }
}
