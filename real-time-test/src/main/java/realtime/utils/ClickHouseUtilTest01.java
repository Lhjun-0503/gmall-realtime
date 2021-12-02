package realtime.utils;


import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import realtime.bean.TransientSink;
import realtime.common.GmallConfig;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtilTest01 {

    public static <T> SinkFunction<T> getSinkFunc(String sql) {

        return JdbcSink.sink(sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                        //通过反射的方式获取对象中的属性
                        Class<?> clz = t.getClass();

                        //获取所有类型的属性，包括私有的
                        Field[] fields = clz.getDeclaredFields();

                        //声明偏移量，如果属性有注解，需要跳过，不写入clickHouse
                        int offset = 0;

                        //遍历属性数组，给预编译对象的占位符赋值
                        for (int i = 0; i < fields.length; i++) {

                            Field field = fields[i];

                            //设置私有属性可以访问
                            field.setAccessible(true);

                            //获取属性注解
                            TransientSink annotation = field.getAnnotation(TransientSink.class);

                            //如果属性存在注解，该属性值不写入clickHouse
                            if (annotation != null) {
                                offset++;
                                continue;
                            }


                            try {

                                //获取属性值
                                Object o = field.get(t);

                                preparedStatement.setObject(i + 1 - offset, o);

                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                },
                new JdbcExecutionOptions
                        .Builder()
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions
                        .JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());

    }

}
