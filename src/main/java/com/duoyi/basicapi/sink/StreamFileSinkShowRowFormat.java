package com.duoyi.basicapi.sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;



/**
 * StreamFileSink 不但可以将数据写入到各种文件系统中，而且整合了 checkpoint 机制来保证 Exacly Once 语义，
 * 还可以对文件进行分桶存储，还支持以列式存储的格式写入，功能更强大。
 * streamFileSink 中输出的文件，其生命周期会经历 3 中状态：
 *      - in-progress Files：正在处理的文件
 *      - Pending Files：处理完成，等待checkpoint完成
 *      - Finished Files：checkpoint完成后，标记为Finished
 *
 * 想要列示存储需要额外添加依赖详见pom文件
 */
public class StreamFileSinkShowRowFormat {
    public static void main(String[] args) throws Exception {

        // 获取一个编程、执行入口环境 env
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        // 通过数据源组件，加载、创建 datastream
        DataStreamSource<String> source = env.fromElements("1","2", "3");
        //对 datastream 调用各种处理算子表达计算逻辑,此处不做处理

        //通过 sink 算子指定计算结果的输出方式
        /**
         * 通过 DefaultRollingPolicy 这个工具类，指定文件滚动生成的策略。这里设置的文件滚动生成策略有两个
         *      一个是距离上一次生成文件时间超过 30 秒
         *      另一个是文件大小达到 100 mb。
         * 这两个条件只要满足其中一个即可滚动生成文件
         */
        DefaultRollingPolicy<String, String> rollingPolicy = DefaultRollingPolicy.create()
                .withRolloverInterval(30 * 1000L) //30 秒滚动生成一个文件
                .withMaxPartSize(1024L * 1024L * 100L) //当文件达到 100m 滚动生成一个文件
                .build();
        StreamingFileSink<String> sink = StreamingFileSink.forRowFormat( //forRowFormat 方法将文件输出目录、文件写入的编码传入，
                        new Path(""), //指的文件存储目录
                        new SimpleStringEncoder<String>("UTF-8")) //指的文件的编码
                .withRollingPolicy(rollingPolicy) //再调用 withRollingPolicy 关联上面的文件滚动生成策略
                .build(); // 接着调用 build 方法构建好StreamingFileSink

        //最后将其作为参数传入到 addSink 方法中。
        source.addSink(sink);

        //在 env 上触发程序提交运行
        env.execute();
    }
}
