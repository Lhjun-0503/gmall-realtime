package com.atguigu.gmall.realtime.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.DimUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSink extends RichSinkFunction<JSONObject> {

    private Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化Phoenix连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);

        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

    }

    //将数据写入Phoenix
    @Override
    public void invoke(JSONObject jsonObject, Context context) throws Exception {
        PreparedStatement preparedStatement = null;

        try {
            //获取数据中的key以及value
            JSONObject data = jsonObject.getJSONObject("after");

            Set<String> keys = data.keySet();

            Collection<Object> values = data.values();

            //获取表名
            String tableName = jsonObject.getString("sink_table");

            //创建插入数据的SQL
            String upsertSql = genUpsertSql(tableName, keys, values);
            System.out.println(upsertSql);

            //编译SQL
            preparedStatement = connection.prepareStatement(upsertSql);

            //执行
            preparedStatement.executeUpdate();

            //提交
            connection.commit();

            //判断如果为更新数据，则删除Redis中的数据
            if ("update".equals(jsonObject.getString("type"))) {
                String value = data.getString("id");

                System.out.println("删除redisKey>>>"+tableName.toUpperCase() + ":" + value);
                DimUtil.delRedisDim(tableName.toUpperCase(),value);

            }

        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("插入Phoenix数据失败！");
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }


    //创建插入数据的SQL upsert into t(id,name,sex) values('...','...','...')
    private String genUpsertSql(String tableName, Set<String> keys, Collection<Object> values) {
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." +
                tableName + "(" + StringUtils.join(keys, ",") + ")" +
                "values( '" + StringUtils.join(values, "','") + "' )";
    }
}