package com.duoyi.basicapi.moreflowoption;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

/**
 * 通常使用方法
 *      将需要广播出去的流，调用 broadcast 方法进行广播转换，得到广播流 BroadCastStream
 *      然后在主流上调用 connect 算子，来连接广播流（以实现广播状态的共享处理）
 *      在连接流上调用 process 算子，就会在同一个 ProcessFunciton 中提供两个方法分别对两个流进行处理，并在这个 ProcessFunction 内实现“广播状态”的共享
 */
public class BroadcastDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        // 获取一个编程、执行入口环境 env
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);

        // 通过数据源组件，加载、创建 datastream
        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9998);

        //对 datastream 调用各种处理算子表达计算逻辑
        // id,eventId
        SingleOutputStreamOperator<Tuple2<String, String>> s1 = stream1.map(s -> {
            String[] arr = s.split(",");
            return Tuple2.of(arr[0], arr[1]);
        }).returns(new TypeHint<Tuple2<String, String>>() {
        });

        // id,age,city
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Tuple3<String, String, String>> s2 = stream2.map(s -> {
            String[] arr = s.split(",");
            return Tuple3.of(arr[0], arr[1], arr[2]);
        }).returns(new TypeHint<Tuple3<String, String, String>>() {
        });

        /**
         * 案例背景：
         * 流 1： 用户行为事件流（持续不断，同一个人也会反复出现，出现次数不定
         * 流 2： 用户维度信息（年龄，城市），同一个人的数据只会来一次，来的时间也不定 （作为广播流）
         * 需要加工流 1，把用户的维度信息填充好，利用广播流来实现
         */

        // 将字典数据所在流： s2 ， 转成 广播流
        MapStateDescriptor<String, Tuple2<String, String>> userInfoStateDesc = new
                MapStateDescriptor<>(
                        "userInfoStateDesc", 
                        TypeInformation.of(String.class), 
                        TypeInformation.of(new TypeHint<Tuple2<String, String>>() {})
                );
        BroadcastStream<Tuple3<String, String, String>> s2BroadcastStream = s2.broadcast(userInfoStateDesc);

        // 哪个流处理中需要用到广播状态数据，就要 去 连接 connect 这个广播流
        BroadcastConnectedStream<Tuple2<String, String>, Tuple3<String, String, String>> connected = s1.connect(s2BroadcastStream);


        /**
         * 对 连接了广播流之后的 ”连接流“ 进行处理
         * 核心思想：
         * 在 processBroadcastElement 方法中，把获取到的广播流中的数据，插入到 “广播状态”中
         * 在 processElement 方法中，对取到的主流数据进行处理（从广播状态中获取要拼接的数据，拼接后输出）
         */
        SingleOutputStreamOperator<String> resultStream = connected.process(new BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {
            /*BroadcastState<String, Tuple2<String, String>> broadcastState;*/

            /**
             * 本方法，是用来处理 主流中的数据（每来一条，调用一次）
             * @param element 左流（主流）中的一条数据
             * @param ctx 上下文
             * @param out 输出器
             * @throws Exception
             */
            @Override
            public void processElement(Tuple2<String, String> element, BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                // 通过 ReadOnlyContext ctx 取到的广播状态对象，是一个 “只读 ” 的对象；
                ReadOnlyBroadcastState<String, Tuple2<String, String>> broadcastState = ctx.getBroadcastState(userInfoStateDesc);
                if (broadcastState != null) {
                    Tuple2<String, String> userInfo = broadcastState.get(element.f0);
                    out.collect(element.f0 + "," + element.f1 + "," + (userInfo == null ? null : userInfo.f0)
                            + "," + (userInfo == null ? null : userInfo.f1));
                } else {
                    out.collect(element.f0 + "," + element.f1 + "," + null + "," + null);
                }
            }

            /**
             *
             * @param element 广播流中的一条数据
             * @param ctx 上下文
             * @param out 输出器
             * @throws Exception
             */
            @Override
            public void processBroadcastElement(Tuple3<String, String, String> element, BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>.Context ctx, Collector<String> out) throws Exception {
                // 从上下文中，获取广播状态对象（可读可写的状态对象）
                BroadcastState<String, Tuple2<String, String>> broadcastState = ctx.getBroadcastState(userInfoStateDesc);
                // 然后将获得的这条 广播流数据， 拆分后，装入广播状态
                broadcastState.put(element.f0, Tuple2.of(element.f1, element.f2));
            }
        });

        //通过 sink 算子指定计算结果的输出方式
        resultStream.print();

        //在 env 上触发程序提交运行
        env.execute();



    }
}
