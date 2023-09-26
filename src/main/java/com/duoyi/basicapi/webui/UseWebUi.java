package com.duoyi.basicapi.webui;

import com.duoyi.basicapi.WindowWordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class UseWebUi {
    public static void main(String[] args) throws Exception {
        /**
         *  获取一个编程、执行入口环境 env
         *  通过数据源组件，加载、创建 datastream
         *  对 datastream 调用各种处理算子表达计算逻辑
         *  通过 sink 算子指定计算结果的输出方式
         *  在 env 上触发程序提交运行
         */

        // 要开启本地 webui 功能，需要添加依赖，详情参考pom文件
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",8081);

        // 获取一个编程、执行入口环境 env，并将配置好webui域名端口的配置类传进env
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setRestartStrategy(RestartStrategies.noRestart());


        // 通过数据源组件，加载、创建 datastream
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);


        // 对 datastream 调用各种处理算子表达计算逻辑,此处不做处理


        // 通过 sink 算子指定计算结果的输出方式
        source.print();

        // 在 env 上触发程序提交运行
        env.execute("window wordcount");

    }
}
