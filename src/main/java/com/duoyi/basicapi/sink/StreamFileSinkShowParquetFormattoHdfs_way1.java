package com.duoyi.basicapi.sink;

import com.duoyi.pojo.EventLog;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

// 要把处理好的数据流，输出到文件系统（hdfs）
// 手动构建 Avro 的 Schema 对象，得到 ParquetWriterFactory 的方式

/**
 * ParquetWriterFactory<GenericRecord> writerFactory = ParquetAvroWriters.forGenericRecord(schema);
 * FileSink sink = FileSink.forBulkFormat(basePath,parquetWriterFactory)
 * 将原始数据转成 GenericRecord 流，输出到 FileSink 算子
 * dataStream.sinkTo(sink)
 */
public class StreamFileSinkShowParquetFormattoHdfs_way1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启 checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        // 构造好一个数据流
        DataStreamSource<EventLog> streamSource = env.fromElements(new EventLog(),new EventLog());



        // 将上面的数据流输出到文件系统（假装成一个经过了各种复杂计算后的结果数据流）
        /**
         * 方式一：
         * 核心逻辑：
         * - 构造一个 schema
         * - 利用 schema 构造一个 parquetWriterFactory
         * - 利用 parquetWriterFactory 构造一个 FileSink 算子
         * - 将原始数据转成 GenericRecord 流，输出到 FileSink 算子
         */
        // 1. 先定义 GenericRecord 的数据模式(schema)
        Schema schema = SchemaBuilder.builder()
                .record("DataRecord")
                .namespace("cn.doitedu.flink.avro.schema")
                .doc("用户行为事件数据模式")
                .fields()
                .requiredInt("gid")
                .requiredLong("ts")
                .requiredString("eventId")
                .requiredString("sessionId")
                .name("eventInfo")
                .type()
                .map()
                .values()
                .type("string")
                .noDefault()
                .endRecord();

        // 2. 通过定义好的 schema 模式，来得到一个 parquetWriter
        ParquetWriterFactory<GenericRecord> writerFactory =
                ParquetAvroWriters.forGenericRecord(schema);

        // 3. 利用生成好的 parquetWriter，来构造一个 支持列式输出 parquet 文件的 sink 算子
        FileSink<GenericRecord> sink1 = FileSink.forBulkFormat(new Path("d:/datasink/"), writerFactory)
                .withBucketAssigner(new DateTimeBucketAssigner<GenericRecord>("yyyy-MM-dd--HH"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("doit_edu").withPartSuffix(".parquet").build())
                .build();


        // 4. 将自定义 javabean 的流，转成 上述 sink 算子中 parquetWriter 所需要的 GenericRecord 流
        SingleOutputStreamOperator<GenericRecord> recordStream = streamSource
                .map((MapFunction<EventLog, GenericRecord>) eventLog -> {
                    // 构造一个 Record 对象
                    GenericData.Record record = new GenericData.Record(schema);
                    // 将数据填入 record
                    record.put("gid", (int) eventLog.getGuid());
                    record.put("eventId", eventLog.getEventId());
                    record.put("ts", eventLog.getTimeStamp());
                    record.put("sessionId", eventLog.getSessionId());
                    record.put("eventInfo", eventLog.getEventInfo());
                    return record;
                }).returns(new GenericRecordAvroTypeInfo(schema));// 由于 avro 的相关类、对象需要用 avro 的序列化器，所以需要显式指定 AvroTypeInfo 来提供 AvroSerializer

        // 5. 输出数据
        recordStream.sinkTo(sink1);
        env.execute();
    }
}
