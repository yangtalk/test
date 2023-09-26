package com.duoyi.basicapi.transformation.easychange;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * flatmap 映射（DataStream → DataStream）
 * FlatMapFunction: x-> x1, x2,x3,x4 [1 条变多条，并展平]
 */
public class FlatMapDemo {
    public static void main(String[] args) throws Exception {
        // 获取一个编程、执行入口环境 env
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();

        // 通过数据源组件，加载、创建 datastream
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);


        //对 datastream 调用各种处理算子表达计算逻辑
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordSplit = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });


        //通过 sink 算子指定计算结果的输出方式
        wordSplit.print();

        //在 env 上触发程序提交运行
        env.execute();


    }
}
