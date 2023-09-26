package com.duoyi.basicapi.transformation.easychange;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import javax.sound.midi.Soundbank;

// filter 过滤（DataStream → DataStream）
// FilterFunction : x -> true/false
public class FilterDemo {
    public static void main(String[] args) throws Exception {
        // 获取一个编程、执行入口环境 env
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();

        // 通过数据源组件，加载、创建 datastream
        DataStreamSource<Integer> source = env.fromElements(1,2,3,4,5,6,7,8);

        //对 datastream 调用各种处理算子表达计算逻辑
        SingleOutputStreamOperator<Integer> filter = source.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer integer) throws Exception {
                return integer % 2 == 0; // ; //过滤掉返回 false 的数组
            }
        });


        //通过 sink 算子指定计算结果的输出方式
        filter.print();

        //在 env 上触发程序提交运行
        env.execute();
    }


}
