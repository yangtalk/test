package com.duoyi.highleverapi.processfunction;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 1. ProcessFunction 继承了RichFunction ， 因而具备了getRuntimeContext() ，open() ，close()
 * 2. flink 提供了大量不同类型的 process function，让其针对不同的 datastream 拥有更具针对性的功能
 *       ProcessFunction （普通 DataStream 上调 process 时）
 *       KeyedProcessFunction （KeyedStream 上调 process 时）
 *       ProcessWindowFunction（WindowedStream 上调 process 时）
 *       ProcessAllWindowFunction（AllWindowedStream 上调 process 时）
 *       CoProcessFuntion （ConnectedStreams 上调 process 时）
 *       ProcessJoinFunction （JoinedStreams 上调 process 时）
 *       BroadcastProcessFunction （BroadCastConnectedStreams 上调 process 时）
 *       KeyedBroadcastProcessFunction（KeyedBroadCastConnectedStreams 上调 process 时）
 */
public class ProcessFunctionsDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        // 获取一个编程、执行入口环境 env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        // 通过数据源组件，加载、创建 datastream
        // id,eventId
        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9998);

        /**
         * 在普通的 datastream 上调用 process 算子，传入的是 "ProcessFunction"
         */
        SingleOutputStreamOperator<Tuple2<String, String>> s1 = stream1.process(new ProcessFunction<String, Tuple2<String, String>>() {
            // 可以使用 生命周期 open 方法
            @Override
            public void open(Configuration parameters) throws Exception {
                // 可以调用 getRuntimeContext 方法拿到各种运行时上下文信息
                RuntimeContext runtimeContext = getRuntimeContext();
                runtimeContext.getTaskName();
                super.open(parameters);
            }


            @Override
            public void processElement(String value, ProcessFunction<String, Tuple2<String, String>>.Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
                // 可以做测流输出
                ctx.output(new OutputTag<String>("s1", Types.STRING), value);
                // 可以做主流输出
                String[] arr = value.split(",");
                out.collect(Tuple2.of(arr[0], arr[1]));
            }

            // 可以使用 生命周期 close 方法
            @Override
            public void close() throws Exception {
                super.close();
            }
        });


        /**
         * 在 keyedStream 上调用 process 算子，传入的是 "KeyedProcessFunction"
         * KeyedProcessFunction 中的
         *      泛型 1： 流中的 key 的类型；
         *      泛型 2： 流中的数据的类型 ；
         *      泛型 3： 处理后的输出结果的类型
         */

        // 对 s1 流进行 keyby 分组
        KeyedStream<Tuple2<String, String>, String> keyedStream = s1.keyBy(tp2 -> tp2.f0);
        // 然后在 keyby 后的数据流上调用 process 算子
        SingleOutputStreamOperator<Tuple2<Integer, String>> s2 = keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<Integer, String>>() {
            @Override
            public void processElement(Tuple2<String, String> value, KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<Integer, String>>.Context ctx, Collector<Tuple2<Integer, String>> out) throws Exception {
                // 把 id 变整数，把 eventId 变大写
                out.collect(Tuple2.of(Integer.parseInt(value.f0), value.f1.toUpperCase()));
            }
        });

        //通过 sink 算子指定计算结果的输出方式
        s2.print();
        //在 env 上触发程序提交运行
        env.execute();
    }
}
