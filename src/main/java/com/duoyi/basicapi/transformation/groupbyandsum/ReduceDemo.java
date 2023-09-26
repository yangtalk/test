package com.duoyi.basicapi.transformation.groupbyandsum;

import com.duoyi.pojo.CountBean;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * reduce：自定义滚动聚合逻辑
 */
public class ReduceDemo {
    public static void main(String[] args) throws Exception {
        // 获取一个编程、执行入口环境 env
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();

        // 通过数据源组件，加载、创建 datastream
        DataStreamSource<CountBean> source = env.fromElements(new CountBean("a",2),new CountBean("a",1),new CountBean("b",3),new CountBean("a",4),new CountBean("b",5),new CountBean("b",2));
        //对 datastream 调用各种处理算子表达计算逻辑
        KeyedStream<CountBean, Tuple> word = source.keyBy("word");
        SingleOutputStreamOperator<CountBean> reduce = word.reduce(new ReduceFunction<CountBean>() {
            @Override
            public CountBean reduce(CountBean t1, CountBean t2) throws Exception {
                t1.setCount(t1.getCount() + t2.getCount()); //将元组对应的次数进行累加
                return t1; //返回累加后的元组
            }
        });
        //通过 sink 算子指定计算结果的输出方式
        reduce.print();

        //在 env 上触发程序提交运行
        env.execute();
    }
}
