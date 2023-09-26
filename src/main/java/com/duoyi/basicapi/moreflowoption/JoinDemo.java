package com.duoyi.basicapi.moreflowoption;

import com.duoyi.pojo.StuInfo;
import com.duoyi.pojo.Student;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 关联两个流（类似于 sql 中 join），需要指定 join 的条件需要在窗口中进行关联后的逻辑计算
 * 代码结构
 * stream.join(otherStream)
 * .where(<KeySelector>)
 * .equalTo(<KeySelector>)
 * .window(<WindowAssigner>)
 * .apply(<JoinFunction>)
 */
public class JoinDemo {
    public static void main(String[] args) throws Exception {
        // 获取一个编程、执行入口环境 env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 通过数据源组件，加载、创建 datastream
        // 数据流 1
        SingleOutputStreamOperator<Student> s1 = env.fromElements(new Student(1, "", "", Float.valueOf("0")));
        // 数据流 2
        SingleOutputStreamOperator<StuInfo> s2 = env.fromElements(new StuInfo());
        // join 两个流，此时并没有具体的计算逻辑
        JoinedStreams<Student, StuInfo> join = s1.join(s2);
        // 对 join 流进行计算处理
        DataStream<String> stream = join.
                // where 流 1 的某字段 equalTo 流 2 的某字段
                where(s -> s.getId()).equalTo(s -> s.getId())
                // join 实质上只能在窗口中进行
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                // 对窗口中满足关联条件的数据进行计算
                .apply(new JoinFunction<Student, StuInfo, String>() {
                    @Override
                    public String join(Student first, StuInfo second) throws Exception {
                        // first: 左流数据 ; second: 右流数据
                        // 计算逻辑
                        // 返回结果
                        return null;
                    }
                });


        //通过 sink 算子指定计算结果的输出方式
        //在 env 上触发程序提交运行
        env.execute();
    }
}
