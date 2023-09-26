package com.duoyi.util;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.Charset;

public class KafkaStringSerializationSchema implements KafkaSerializationSchema<String>  {
    private String topic;
    private String charset;

    public KafkaStringSerializationSchema(String topic){
        this(topic,"UTF-8");
    }

    public KafkaStringSerializationSchema(String topic,String charset){
        this.topic = topic;
        this.charset = charset;
    }

    // 调用该方法将数据进行序列化
    @Override
    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
        //将数据转成 bytes 数组
        byte[] bytes = element.getBytes(Charset.forName(charset));
        //返回 ProducerRecord
        return new ProducerRecord<>(topic, bytes);
    }
}
