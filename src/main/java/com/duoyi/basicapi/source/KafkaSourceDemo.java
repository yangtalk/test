package com.duoyi.basicapi.source;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Duration;
import java.util.Properties;

public class KafkaSourceDemo {
    public static void main(String[] args) {

        // 获取流处理环境
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();

        // 通过数据源组件，加载、创建 datastream
        // 1. 首先在 maven 项目的 pom.xml 文件中导入 Flink 跟 Kafka 整合的依赖,详情见pom文件
        // 2. Kafka配置有新老版本之分。
        // 2.1 老版本配置示例 --- > 式虽然可以记录偏移量，但是无法保证 Exactly Once；
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092"); //设置 Kafka 的地址和端口
        properties.setProperty("group.id","consumer-group");//设置消费者组 ID
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // 设置Key的序列化器，用于将Kafka中的数据转为字节流后传输
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");// 设置Value的序列化器，用于将Kafka中的数据转为字节流后传输
        properties.setProperty("auto.offset.reset","latest");//读取偏移量策略：如果没有记录偏移量，就从头读，如果记录过偏移量，就接着读
        properties.setProperty("enable.auto.commit", "true");//没有开启 checkpoint，让 flink 提交偏移量的消费者定期自动提交偏移量

        // kafkaSource
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("tpc1", new SimpleStringSchema(), properties);
        env.addSource(kafkaSource);



        // 2.2 新版本配置示例 --- > 消费位移记录在算子状态中，实现了消费位移状态的容错，从而可以支持端到端的exactly
        KafkaSource<String> kafkaSource2 = KafkaSource.<String>builder()
                .setBootstrapServers("doit01:9092,doit02:9092,doit03:9092")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setGroupId("gp001")
                .setTopics("doitedu")
                // 要求开启flink的checkpoint机制，flink内部是把consumer的消费位移记录在自己的checkpoint存储中；
                // 如果没有开启checkpoint，也可以继续使用kafka的原生偏移量自动提交机制，需要配置如下两个参数
                //.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true")
                //.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"100")

                // OffsetsInitializer.earliest() 从最小偏移量开始消费
                // OffsetsInitializer.earliest() 从最新偏移量开始消费
                // OffsetsInitializer.offsets(Map<TopicPartition,offset>)  从指定的偏移量开始消费
                // OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST) 从上次记录的消费位置开始接着消费（如果没有之前记录好的偏移量，则将消费起始位置重置为latest）
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .build();

        // 设置水位线策略
        WatermarkStrategy<String> strategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        String[] arr = element.split(",");
                        return Long.parseLong(arr[3]);
                    }
                });

        // 将水位线策略与kafkaSource进行绑定，使消费位移记录在算子状态中，实现了消费位移状态的容错
        DataStreamSource<String> stream1 = env.fromSource(kafkaSource2,strategy,"").setParallelism(2);
    }
}
