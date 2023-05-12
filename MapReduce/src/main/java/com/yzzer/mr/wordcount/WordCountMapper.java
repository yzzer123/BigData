package com.yzzer.mr.wordcount;


import edu.stanford.nlp.simple.Document;
import edu.stanford.nlp.simple.Sentence;
import edu.stanford.nlp.simple.Token;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

/**
 * wordcount为例
 * 自定义Mapper类 需要继承Hadoop提供的Mapper，并且根据业务需求指定输入数据和输出数据类型
 * 输入数据类型
 * KEYIN  读取数据的偏移量 数字 LongWritable
 * VALUEIN 读取数据的一行数据  文本 Text
 * 输出数据类型
 * KEYOUT   输出数据KEY类型  就是一个单词 Text
 * VALUEOUT 输出数据的Value类型 给单词的标记 IntWritable
 */

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text outk = new Text();
    private IntWritable outv = new IntWritable(1);

    /**
     * Map端的核心业务处理方法   每输入一行数据就调用一次map方法
     * @param key       输入数据的key
     * @param value     输入数据的value
     * @param context   上下文对象
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取当前输入的数据
//        String[] words = value.toString().trim().split("[^a-zA-Z]+");
//        for(String word: words){
//            if(word.length() < 2){
//                continue;
//            }
//            outk.set(word.toLowerCase());
//            context.write(outk, outv);
//        }

        // 对文本分句，对于非法字符进行替换
        Document document = new Document(value.toString().replaceAll("[^a-zA-Z]+", " "));

        // 分词后判断词性
        for (Sentence sentence : document.sentences()) {
            for (Token token : sentence.tokens()) {
                if (token.posTag().startsWith("NN") && token.word().length()>1){
                    outk.set(token.lemma().toLowerCase());
                    context.write(outk, outv);
                }
            }
        }
    }
}
