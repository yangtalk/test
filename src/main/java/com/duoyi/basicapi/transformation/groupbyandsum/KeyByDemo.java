package com.duoyi.basicapi.transformation.groupbyandsum;

import com.duoyi.pojo.CountBean;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * keyBy 按 key 分组（DataStream → KeyedStream）
 */
public class KeyByDemo {
    public static void main(String[] args) throws Exception {
        // 获取一个编程、执行入口环境 env
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();

        // 通过数据源组件，加载、创建 datastream
        DataStreamSource<CountBean> source = env.fromElements(new CountBean("a",2),new CountBean("a",1),new CountBean("b",3),new CountBean("a",4),new CountBean("b",5),new CountBean("b",2));
        //对 datastream 调用各种处理算子表达计算逻辑
        KeyedStream<CountBean, Tuple> word = source.keyBy("word");

        //通过 sink 算子指定计算结果的输出方式
        word.print();

        //在 env 上触发程序提交运行
        env.execute();
    }
}
