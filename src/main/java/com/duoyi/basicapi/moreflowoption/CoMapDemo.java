package com.duoyi.basicapi.moreflowoption;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * coMap（ConnectedStreams → DataStream）
 * ConnectedStreams 调用 map 方法时需要传入 CoMapFunction 函数；
 *    -该接口需要指定 3 个泛型
 *          1 第一个输入 DataStream 的数据类型
 *          2 第二个输入 DataStream 的数据类型
 *          3 返回结果的数据类型。
 *    需要重写两个方法：这两个方法必须是相同的返回值类型。
 *          1 map1 方法，是对第 1 个流进行 map 的处理逻辑。
 *          2 map2 方法，是对 2 个流进行 map 的处理逻辑
 */

public class CoMapDemo {
    public static void main(String[] args) throws Exception {
        // 获取一个编程、执行入口环境 env
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        // 通过数据源组件，加载、创建 datastream
        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3);
        DataStreamSource<String> source2 = env.fromElements("a", "b", "c");

        //对 datastream 调用各种处理算子表达计算逻辑
        // 利用connect将两条流合并到一起
        ConnectedStreams<Integer, String> connectedStreams = source1.connect(source2);
        SingleOutputStreamOperator<String> mapStream = connectedStreams.map(new CoMapFunction<Integer, String, String>() {
            //对 ConnectedStreams 中两个流分别调用个不同逻辑的 map 方法
            @Override
            public String map1(Integer value) throws Exception {
                return String.valueOf(value * 10); //第一个 map 方将是第二个流的数字乘以 10 并转成 String
            }

            @Override
            public String map2(String value) throws Exception {
                return value.toUpperCase(); //第二个 map 方法是将第一个流的字符变大写
            }
        });


        //通过 sink 算子指定计算结果的输出方式
        //在 env 上触发程序提交运行
        env.execute();
    }
}
