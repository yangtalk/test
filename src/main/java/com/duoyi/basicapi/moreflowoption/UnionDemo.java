package com.duoyi.basicapi.moreflowoption;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 两个或者多个数据类型一致的 DataStream 合并成一个 DataStream
 */
public class UnionDemo {
    public static void main(String[] args) throws Exception {
        // 获取一个编程、执行入口环境 env
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        // 通过数据源组件，加载、创建 datastream
        DataStreamSource<String> source1 = env.fromElements("1", "2", "3");
        DataStreamSource<String> source2 = env.fromElements("a", "b", "c");

        //对 datastream 调用各种处理算子表达计算逻辑
        // 利用connect将两条流合并到一起
        DataStream<String> union = source1.union(source2);

        //通过 sink 算子指定计算结果的输出方式
        union.print();
        //在 env 上触发程序提交运行
        env.execute();
    }
}
