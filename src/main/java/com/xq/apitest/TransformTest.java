package com.xq.apitest;

import com.xq.apitest.pojo.SensorReading1;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("D:\\code\\FlinkTutorial_1.10\\src\\main\\resources\\sensor.txt");
        SingleOutputStreamOperator<SensorReading1> out = source.map((MapFunction<String, SensorReading1>) value -> {
            String[] split = value.split(",");
            return new SensorReading1(split[0].trim(), Long.parseLong(split[1].trim()), Double.parseDouble(split[2].trim()));
        });

        SingleOutputStreamOperator<SensorReading1> minby = out.keyBy((KeySelector<SensorReading1, String>) value -> value.getId())
                .minBy("temperature");

        minby.print("minby");

        env.execute();


    }
}
