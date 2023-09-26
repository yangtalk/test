package com.duoyi.basicapi.sink;

import com.duoyi.util.KafkaStringSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaSinkOld {
    public static void main(String[] args) throws Exception {
        // 获取一个编程、执行入口环境 env
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        // 通过数据源组件，加载、创建 datastream
        DataStreamSource<String> source = env.fromElements("1", "2", "3");
        //对 datastream 调用各种处理算子表达计算逻辑,此处不做处理

        //通过 sink 算子指定计算结果的输出方式
        //写入 Kafka 的 topic
        String topic = "test";
        //设置 Kafka 相关参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node-1:9092,node-2:9092,node-3:9092");

        FlinkKafkaProducer<String> flinkKafkaProducer = new FlinkKafkaProducer<>(
                topic,//指定 topic
                new KafkaStringSerializationSchema(topic),//指定写入 Kafka 的序列化 Schema(此处自定义，参考util包下)
                properties,//指定 Kafka 的相关参数
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE //指定写入 Kafka 为 EXACTLY_ONCE 语义
        );
        source.addSink(flinkKafkaProducer);

        //在 env 上触发程序提交运行
        env.execute();
    }
}
