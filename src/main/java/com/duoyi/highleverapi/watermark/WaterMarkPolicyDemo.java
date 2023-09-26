package com.duoyi.highleverapi.watermark;

import com.duoyi.pojo.Score;
import com.duoyi.pojo.Student;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WaterMarkPolicyDemo {
    public static void main(String[] args) throws Exception {
        // 获取一个编程、执行入口环境 env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 通过数据源组件，加载、创建 datastream
        DataStreamSource<Student> source = env.fromElements(new Student());
        //对 datastream 调用各种处理算子表达计算逻辑
        // 构造一个乱序有延迟的 watermark 生成策略
        WatermarkStrategy<Score> scoreWatermarkStrategy = WatermarkStrategy
                .<Score>forBoundedOutOfOrderness(Duration.ofMillis(2000))// 根据实际数据的最大乱序情况来设置
                .withTimestampAssigner(
                        new SerializableTimestampAssigner<Score>() {
                            @Override
                            public long extractTimestamp(Score element, long recordTimestamp) {
                                return element.getTimeStamp();
                            }
                        })
                // 防止上游某些分区的水位线不推进导致下游的窗口一直不触发（这个分区很久都没数据）
                .withIdleness(Duration.ofMillis(3000));

        // 构造单并行度流 1
        DataStreamSource<Score> source1 = env.fromElements(new Score());
        SingleOutputStreamOperator<Score> s1 = source1.assignTimestampsAndWatermarks(scoreWatermarkStrategy);
        // 构造单并行度流 2
        DataStreamSource<Score> source2 = env.fromElements(new Score());
        SingleOutputStreamOperator<Score> s2 = source2.assignTimestampsAndWatermarks(scoreWatermarkStrategy);
        // 两条单并行度流，合并到一条单并行度流
        DataStream<Score> s = s1.union(s2);
        // 打印 watermark 信息，观察 watermark 推进情况
        s.process(new ProcessFunction<Score, Score>() {
            @Override
            public void processElement(Score value, Context ctx, Collector<Score> out) throws Exception
            {
                // 获取当前的 watermark（注意：此处所谓当前 watermark 是指处理当前数据前的 watermark）
                long currentWatermark = ctx.timerService().currentWatermark();
                Long timestamp = ctx.timestamp();
                System.out.println("s"+ " : " + timestamp + " : " + currentWatermark + " => "+value);
                out.collect(value);
            }
        }).print();

        env.execute();
    }
}
