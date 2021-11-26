package com.atguigu.gmall.realtime.utils;

import com.atguigu.gmall.realtime.bean.TransientSink;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Desc: 操作ClickHouse的工具类
 */
public class ClickhouseUtil {

    public static <T> SinkFunction<T> getSinkFunc(String sql) {
        return JdbcSink.sink(sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                        //通过反射的方式获取数据中的内容
                        //1.获取所有的列信息(包含私有属性)
                        Field[] declaredFields = t.getClass().getDeclaredFields();

                        //2.遍历列信息
                        int offset = 0;
                        for (int i = 0; i < declaredFields.length; i++) {
                            Field field = declaredFields[i];

                            //设置私有属性信息可获取
                            field.setAccessible(true);

                            //获取字段上的注解信息
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);

                            if (transientSink != null) {
                                offset++;
                                continue;
                            }

                            try {
                                Object o = field.get(t);
                                preparedStatement.setObject(i + 1 - offset, o);
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }

                    }
                }, new JdbcExecutionOptions
                        .Builder()
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());
    }

}
