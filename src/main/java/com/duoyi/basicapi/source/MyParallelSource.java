package com.duoyi.basicapi.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 自定义 source 可以实现 SourceFunction 或者 RichSourceFunction , 这两者都是非并行的 source 算子
 * 也可实现 ParallelSourceFunction 或者 RichParallelSourceFunction , 这两者都是可并行的
 * source 算子
 * -- 带 Rich 的，都拥有 open() ,close() ,getRuntimeContext() 方法
 * -- 带 Parallel 的，都可多实例并行执行
 */
public class MyParallelSource extends RichParallelSourceFunction<String> {
    private int i = 1; //定义一个 int 类型的变量，从 1 开始
    private boolean flag = true; //定义一个 flag 标标志


    //run 方法就是用来读取外部的数据或产生数据的逻辑
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (i < 10 && flag){
            Thread.sleep(1000); //为避免太快，睡眠 1 秒
            ctx.collect("data: " + i++); //将数据通过 SourceContext 收集起来
        }
    }

    //cancel 方法就是让 Source 停止
    @Override
    public void cancel() {
        //将 flag 设置成 false，即停止 Source
        flag = false;
    }

    public static void main(String[] args) throws Exception {
        // 获取一个编程、执行入口环境 env
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 通过数据源组件，加载、创建 datastream
        DataStreamSource<String> streamSource = env.addSource(new MyParallelSource());

        // 对 datastream 调用各种处理算子表达计算逻辑,此处不做处理

        // 通过 sink 算子指定计算结果的输出方式
        streamSource.print();

        // 在 env 上触发程序提交运行
        env.execute("FromCollectionDemo");
    }
}
