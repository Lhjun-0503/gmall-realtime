package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PhoenixUtil {
    //声明连接
    private static Connection connection = null;

    //创建phoenix连接
    public static Connection getConnection() {
        try {
            Class.forName(GmallConfig.PHOENIX_DRIVER);

            return DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("获取连接失败！");
        }
    }

    //查询Phoenix数据的方法
    public static <T> List<T> queryList(String sql, Class<T> clz, boolean underScoreCamel) {
        //创建结果集合
        ArrayList<T> list = new ArrayList<>();

        PreparedStatement preparedStatement = null;

        try {

            if (connection == null) {
                connection = getConnection();
            }

            //预编译SQL
            preparedStatement = connection.prepareStatement(sql);

            //执行查询
            ResultSet resultSet = preparedStatement.executeQuery();

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            //解析查询结果
            while (resultSet.next()) {

                //构建泛型对象
                T t = clz.newInstance();

                for (int i = 1; i < columnCount + 1; i++) {

                    //取出列名
                    String columnName = metaData.getColumnName(i);

                    if (underScoreCamel) {
                        columnName = CaseFormat.LOWER_UNDERSCORE.converterTo(CaseFormat.LOWER_CAMEL).convert(columnName);
                    }

                    //取出数据
                    Object value = resultSet.getObject(i);

                    //将值设置给对象
                    BeanUtils.setProperty(t,columnName,value);
                }

                //将当前对象添加至集合
                list.add(t);
            }

            //返回数据

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        return list;
    }

    public static void main(String[] args) {
        System.out.println(queryList("select * from GMALL2021_REALTIME.BASE_TRADEMARK",
                JSONObject.class,
                true));
    }
}
