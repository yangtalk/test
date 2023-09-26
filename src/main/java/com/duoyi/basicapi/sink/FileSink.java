package com.duoyi.basicapi.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileSink {
    public static void main(String[] args) throws Exception {
        // 获取一个编程、执行入口环境 env
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        // 通过数据源组件，加载、创建 datastream
        DataStreamSource<String> source = env.fromElements("1","2", "3");
        //对 datastream 调用各种处理算子表达计算逻辑,此处不做处理

        //通过 sink 算子指定计算结果的输出方式
        // writeAsText 以文本格式输出,实时输出，文件名称是该 Sink 所在subtask的Index + 1,指定了WriteMode.NO_OVERWRITE则会覆盖原文件。
        source.writeAsText("file:///Users/xing/Desktop/text", FileSystem.WriteMode.OVERWRITE);

        // writeAsCsv 以 csv 格式输出：并非实时刷写，达到BufferedOutputStream，默认缓存的大小为 4096个字节，只有达到这个大小，才会 flush 到磁盘，又或者是close后刷鞋
        source.writeAsCsv("");

        // writeUsingOutputFormat 以指定的格式输出到指定目录，需要传入一个 OutputFormat 接口的实现类


        // writeToSocket 输出到网络端口
        source.writeToSocket("localhost", 9999, new SimpleStringSchema());

        //在 env 上触发程序提交运行
        env.execute();
    }
}
