package com.yzzer.mr.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.*;

/**
 * 测试文件的压缩和解压缩功能
 */
public class CompressTest {
    // 准备原始文件
    private final String destFile = "/Users/yzzer/Documents/技术文档/hadoop/07_测试数据/jianai/newja.txt";

    private final String srcFile = "/Users/yzzer/lib/compress.txt.deflate";


    /**
     * 压缩:  通过一个具备压缩功能的输出流将文件写出
     */
    @Test
    public void testCompress() throws IOException, ClassNotFoundException {
        // 声明一个输入流
        FileInputStream in = new FileInputStream(new File(srcFile));

        Configuration conf = new Configuration();

        String classPath = "org.apache.hadoop.io.compress.DefaultCodec";

        Class<?> codeClass = Class.forName(classPath);

        // 获取一个编解码器
        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codeClass, conf);


        // 声明一个输出流 将文件输出
        FileOutputStream out = new FileOutputStream(new File(destFile+codec.getDefaultExtension()));

        // 将普通输出流让编解码器包装
        CompressionOutputStream outStream = codec.createOutputStream(out);

        // 读写数据
        IOUtils.copyBytes(in, outStream, conf);

        // 关闭流
        IOUtils.closeStream(in);
        IOUtils.closeStream(outStream);

    }


    /**
     * 解压缩
     */
    @Test
    public void testUnCompress() throws IOException, ClassNotFoundException {


        Configuration conf = new Configuration();

        String classPath = "org.apache.hadoop.io.compress.DefaultCodec";

        Class<?> codeClass = Class.forName(classPath);

        // 获取一个编解码器
        CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(new Path(srcFile));

        // 声明一个输入流
        FileInputStream in = new FileInputStream(new File(srcFile));


        // 声明一个输出流 将文件输出
        FileOutputStream out = new FileOutputStream(new File(destFile));

        // 将普通输出流让编解码器包装
        CompressionInputStream inputStream = codec.createInputStream(in);

        // 读写数据
        IOUtils.copyBytes(inputStream, out, conf);

        // 关闭流
        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(out);

    }
}
