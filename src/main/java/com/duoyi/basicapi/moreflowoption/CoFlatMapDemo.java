package com.duoyi.basicapi.moreflowoption;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

/**
 * coMap（ConnectedStreams → DataStream）
 * ConnectedStreams 调用 map 方法时需要传入 CoMapFunction 函数；
 *    -该接口需要指定 3 个泛型
 *          1 第一个输入 DataStream 的数据类型
 *          2 第二个输入 DataStream 的数据类型
 *          3 返回结果的数据类型。
 *    需要重写两个方法：这两个方法必须是相同的返回值类型。
 *          1 flatMap1 方法，是对第 1 个流进行 flatMap 的处理逻辑；
 *          2 flatMap2 方法，是对 2 个流进行 flatMap 的处理逻辑；
 */

public class CoFlatMapDemo {
    public static void main(String[] args) throws Exception {
        // 获取一个编程、执行入口环境 env
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        // 通过数据源组件，加载、创建 datastream
        DataStreamSource<String> source1 = env.fromElements("1,2,3","4,5,6");
        DataStreamSource<String> source2 = env.fromElements("a b c", "d e f");

        //对 datastream 调用各种处理算子表达计算逻辑
        // 利用connect将两条流合并到一起
        ConnectedStreams<String, String> connectedStreams = source1.connect(source2);
        SingleOutputStreamOperator<String> outputStream  = connectedStreams.flatMap(new CoFlatMapFunction<String, String, String>() {
            @Override
            public void flatMap1(String value, Collector<String> out) throws Exception {
                String[] nums = value.split(",");
                for (String num : nums) {
                    out.collect(num);
                }
            }

            @Override
            public void flatMap2(String value, Collector<String> out) throws Exception {
                String[] s = value.split(" ");
                for (String s1 : s) {
                    out.collect(s1);
                }
            }
        });

        //通过 sink 算子指定计算结果的输出方式
        outputStream.print();
        //在 env 上触发程序提交运行
        env.execute();
    }
}
