package com.duoyi.basicapi.moreflowoption;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * split 拆分（DataStream → SplitStream）
 * 该方法是将一个 DataStream 中的数据流打上不同的标签，逻辑的拆分成多个不同类型的流，返回一个新的 SplitStream，本质上还是一个数据流，只不过是将流中的数据打上了不同的标签
 *
 * 1.12 版本已删除了 split 和 select 方法，可以使用侧流输出代替
 */
public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        // 获取一个编程、执行入口环境 env
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        // 通过数据源组件，加载、创建 datastream
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3);

        //对 datastream 调用各种处理算子表达计算逻辑,将数据打上标签，拆分成奇数和偶数
        // 定义用于记录偶数的测输出流
        OutputTag<Integer> even = new OutputTag<Integer>("even"){};
        SingleOutputStreamOperator<Integer> mainStream = source.process(new ProcessFunction<Integer, Integer>() {
            @Override
            public void processElement(Integer value, ProcessFunction<Integer, Integer>.Context ctx, Collector<Integer> out) throws Exception {
                if (value % 2 == 0) {
                    // 偶数输出到输出流
                    ctx.output(even, value);
                } else {
                    // 奇数输出到主流
                    out.collect(value);
                }
            }
        });

        // 通过主流获取测输出流
        DataStream<Integer> evenStream = mainStream.getSideOutput(even);


        // 获取
        //通过 sink 算子指定计算结果的输出方式
        evenStream.print();
        //在 env 上触发程序提交运行
        env.execute();
    }
}
