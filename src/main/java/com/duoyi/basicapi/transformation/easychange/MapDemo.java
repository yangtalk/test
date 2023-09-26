package com.duoyi.basicapi.transformation.easychange;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * map 映射（DataStream → DataStream）
 * MapFunction: （x）-> y [1 条变 1 条]
 */
public class MapDemo {
    public static void main(String[] args) throws Exception {
        // 获取一个编程、执行入口环境 env
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();

        //通过数据源组件，加载、创建 datastream
        DataStreamSource<String> source = env.fromElements("hadoop", "spark", "flink", "hbase", "flink", "spark");

        //对 datastream 调用各种处理算子表达计算逻辑
        SingleOutputStreamOperator<String> words = source.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s.toUpperCase();
            }
        });

        //通过 sink 算子指定计算结果的输出方式
        words.print();

        //在 env 上触发程序提交运行
        env.execute();
    }
}
