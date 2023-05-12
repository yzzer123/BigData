package com.yzzer.mr.mywordcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartionner<K, V> extends Partitioner<K, V> {

    @Override
    public int getPartition(K k, V v, int numPartitions) {
        int type = 0;
        if(k instanceof Text){
            String prefix;
            prefix = k.toString().substring(0, 3);
            try{
                type = Integer.parseInt(prefix) % numPartitions;
            }catch (NumberFormatException ignored){
            }
        }
        return type;
    }
}
