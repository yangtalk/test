package com.duoyi.basicapi.sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * sink 算子是将计算结果最终输出的算子
 */
public class PrintSink {
    public static void main(String[] args) throws Exception {
        // 获取一个编程、执行入口环境 env
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        // 通过数据源组件，加载、创建 datastream
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3);
        //对 datastream 调用各种处理算子表达计算逻辑,此处不做处理

        //通过 sink 算子指定计算结果的输出方式
        source.print();
        //在 env 上触发程序提交运行
        env.execute();
    }
}
