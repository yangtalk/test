package com.duoyi.basicapi.sink;

import com.duoyi.pojo.Student;
import com.mysql.jdbc.jdbc2.optional.MysqlXADataSource;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.function.SerializableSupplier;

import javax.sql.XADataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 需要导入依赖，详见pom文件参数
 * 构造一个 jdbc 的 sink 分为开启mysql事务保证一致性的JDBCSink以及未开启事务不能保证精确一次的JDBCSink
 */
public class JdbcSinkDemo {
    public static void main(String[] args) throws Exception {
        // 获取一个编程、执行入口环境 env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 通过数据源组件，加载、创建 datastream
        DataStreamSource<Student> source = env.fromElements(new Student());

        //对 datastream 调用各种处理算子表达计算逻辑,此处不做处理
        // 该 sink 的底层并没有去开启 mysql 的事务，所以并不能真正保证 端到端的 精确一次
        SinkFunction<Student> jdbcSink = JdbcSink.sink(
                "insert into flink_stu values (?,?,?,?) on duplicate key update name=?,gender=?,score=? ",
                new JdbcStatementBuilder<Student>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Student student) throws SQLException {
                        preparedStatement.setInt(1, student.getId());
                        preparedStatement.setString(2, student.getName());
                        preparedStatement.setString(3, student.getGender());
                        preparedStatement.setFloat(4, (float) student.getScore());
                        preparedStatement.setString(5, student.getName());
                        preparedStatement.setString(6, student.getGender());
                        preparedStatement.setFloat(7, (float) student.getScore());
                    }
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://doit01:3306/abc")
                        .withUsername("root")
                        .withPassword("ABC123.abc123")
                        .build());


        // 开启mysql事务能保证精确一次的JDBCSink
        SinkFunction<Student> exactlyOnceSink = JdbcSink.exactlyOnceSink(
                "insert into flink_stu values (?,?,?,?) on duplicate key update name=?,gender=?,score=? ",
                /*"insert into flink_stu values (?,?,?,?) ",*/
                (PreparedStatement preparedStatement, Student student) -> {
                    preparedStatement.setInt(1, student.getId());
                    preparedStatement.setString(2, student.getName());
                    preparedStatement.setString(3, student.getGender());
                    preparedStatement.setFloat(4, (float) student.getScore());
                    preparedStatement.setString(5, student.getName());
                    preparedStatement.setString(6, student.getGender());
                    preparedStatement.setFloat(7, (float) student.getScore());
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(1)
                        .build(),
                JdbcExactlyOnceOptions.builder()
                        .withTransactionPerConnection(true)// mysql 不支持同一个连接上存在并行的多个事务，必须把该参数设置为 true
                        .build(),
                new SerializableSupplier<XADataSource>() {
                    @Override
                    public XADataSource get() {
                        // XADataSource 就是 jdbc 连接，不过它是支持分布式事务的连接
                        // 而且它的构造方法，不同的数据库构造方法不同
                        MysqlXADataSource xaDataSource = new MysqlXADataSource();
                        xaDataSource.setUrl("jdbc:mysql://doit01:3306/abc");
                        xaDataSource.setUser("root");
                        xaDataSource.setPassword("ABC123.abc123");
                        return xaDataSource;
                    }
                }
        );

        //通过 sink 算子指定计算结果的输出方式
        source.addSink(exactlyOnceSink);
        //在 env 上触发程序提交运行
        env.execute();
    }
}
