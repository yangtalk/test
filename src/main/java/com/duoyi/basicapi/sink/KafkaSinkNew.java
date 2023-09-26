package com.duoyi.basicapi.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaSinkNew {
    public static void main(String[] args) throws Exception {
        // 获取一个编程、执行入口环境 env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 通过数据源组件，加载、创建 datastream
        DataStreamSource<String> source = env.fromElements("1", "2", "3");
        //对 datastream 调用各种处理算子表达计算逻辑,此处不做处理

        //通过 sink 算子指定计算结果的输出方式
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("doit01:9092,doit02:9092,doit03:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("topic-name")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setTransactionalIdPrefix("doitedu-")
                .build();
        source.sinkTo(kafkaSink);
        //在 env 上触发程序提交运行
        env.execute();
    }
}
