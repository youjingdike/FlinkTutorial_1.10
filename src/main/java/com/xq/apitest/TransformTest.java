package com.xq.apitest;

import com.xq.apitest.pojo.SensorReading1;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从文件读取数据
        DataStreamSource<String> inputStream = env.readTextFile("D:\\code\\FlinkTutorial_1.10\\src\\main\\resources\\sensor.txt");

        // 1. 基本转换操作：map成样例类类型
        SingleOutputStreamOperator<SensorReading1> dataStream = inputStream.map((MapFunction<String, SensorReading1>) value -> {
            String[] split = value.split(",");
            return new SensorReading1(split[0].trim(), Long.parseLong(split[1].trim()), Double.parseDouble(split[2].trim()));
        });
        // 2. 聚合操作，首先按照id做分组，然后取当前id的最小温度
        SingleOutputStreamOperator<SensorReading1> minby = dataStream.keyBy((KeySelector<SensorReading1, String>) value -> value.getId())
                .minBy("temperature");

        // 3. 复杂聚合操作，reduce，得到当前id最小的温度值，以及最新的时间戳+1
        //如果是某个key的第一条数据，不执行该方法
        SingleOutputStreamOperator<SensorReading1> reduce = dataStream.keyBy("id")
                .reduce((ReduceFunction<SensorReading1>) (cur, newData) -> {
                    System.out.println("cur:"+cur);
                    System.out.println("new:"+newData);
                    return new SensorReading1(cur.getId(), newData.getTimestamp() + 1, Math.min(cur.getTemperature(), newData.getTemperature()));
                });

//        minby.print("minby");
        reduce.print("reduce");
        env.execute();


    }
}
