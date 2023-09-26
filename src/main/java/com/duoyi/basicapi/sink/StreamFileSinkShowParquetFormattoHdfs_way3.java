package com.duoyi.basicapi.sink;

import com.duoyi.pojo.EventLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

// 直接利用普通 JavaBean，利用工具自身的反射机制，得到 ParquetWriterFactory 的方式；

/**
 *  ParquetWriterFactory<EventLog> parquetWriterFactory = ParquetAvroWriters.forReflectRecord(EventLog.class);
 * FileSink sink = FileSink.forBulkFormat(basePath,parquetWriterFactory)
 * dataStream.sinkTo(sink)
 */
public class StreamFileSinkShowParquetFormattoHdfs_way3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启 checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        // 构造好一个数据流
        DataStreamSource<EventLog> streamSource = env.fromElements(new EventLog(), new EventLog());


        // 将上面的数据流输出到文件系统（假装成一个经过了各种复杂计算后的结果数据流）
        /**
         * 方式三：
         * 核心逻辑：
         *      - 利用自己的 JavaBean 类，来构造一个 parquetWriterFactory
         *      - 利用 parquetWriterFactory 构造一个 FileSink 算子
         *      - 将原始数据流，输出到 FileSink 算子
         */

        // 1. 通过自己的 JavaBean 类，来得到一个 parquetWriter
        ParquetWriterFactory<EventLog> parquetWriterFactory =
                ParquetAvroWriters.forReflectRecord(EventLog.class);

        // 2. 利用生成好的 parquetWriter，来构造一个 支持列式输出 parquet 文件的 sink 算子
        FileSink<EventLog> bulkSink = FileSink.forBulkFormat(new Path("d:/datasink3/"),
                        parquetWriterFactory)
                .withBucketAssigner(new DateTimeBucketAssigner<EventLog>("yyyy-MM-dd--HH"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("doit_edu").withPartSuffix(".parquet").build())
                .build();

        // 3. 输出数据
        streamSource.sinkTo(bulkSink);
        env.execute();
    }
}
