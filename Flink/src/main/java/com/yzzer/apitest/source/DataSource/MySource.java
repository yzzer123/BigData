package com.yzzer.apitest.source.DataSource;

import com.yzzer.apitest.source.beans.SensorReading;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.*;

public class MySource implements SourceFunction<SensorReading> {

    // 定义一个标志位来控制数据的产生
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {

        // 随机数发生器
        Random random = new Random();

        HashMap<Integer, Double> sensorTempMap = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            sensorTempMap.put(i + 1, random.nextGaussian() * 20);
        }


        

        while (isRunning){
            // 在当前温度基础上做随机波动
            for (Integer id : sensorTempMap.keySet()) {
                Double nextTemp = sensorTempMap.get(id) + random.nextGaussian();
                sensorTempMap.put(id, nextTemp);
                ctx.collect((new SensorReading("sensor_" + id, System.currentTimeMillis(), nextTemp)));
            }
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
