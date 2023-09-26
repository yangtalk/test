package com.duoyi.basicapi.moreflowoption;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * coGroup 本质上是上述 join 算子的底层算子；功能类似
 */
public class CoGroupDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env   = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream1 = env.fromElements("1,aa,m,18", "2,bb,m,28", "3,cc,f,38");
        DataStreamSource<String> stream2 = env.fromElements("1:aa:m:18", "2:bb:m:28", "3:cc:f:38");
        DataStream<String> res = stream1.coGroup(stream2).where(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {
                        return value;
                    }
                }).equalTo(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {
                        return value;
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<String, String, String>() {
                    @Override
                    public void coGroup(Iterable<String> first, Iterable<String> second, Collector<String> out)
                            throws Exception {
                    }
                });
    }
}
