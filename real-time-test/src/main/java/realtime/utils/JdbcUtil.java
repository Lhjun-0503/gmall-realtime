package realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import realtime.common.GmallConfig;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {

    /**
     *
     * @param sql
     * @param connection
     * @param clz
     * @param undertScoreToCamel
     * @param <T>
     * @return
     * @throws SQLException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws InvocationTargetException
     */
    public static <T> List<T> querySql(String sql, Connection connection, Class<T> clz, boolean undertScoreToCamel) throws SQLException, IllegalAccessException, InstantiationException, InvocationTargetException {

        //创建返回结果的集合，集合中的一个对象的所有属性表示一行数据
        ArrayList<T> result = new ArrayList<>();

        //创建预编译对象
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        //获取结果集
        ResultSet resultSet = preparedStatement.executeQuery();

        //获取结果集的元数据
        ResultSetMetaData metaData = resultSet.getMetaData();

        //获取查询结果的列的数量
        int columnCount = metaData.getColumnCount();

        while (resultSet.next()) {

            //创建对象
            T t = clz.newInstance();

            //遍历resultSet，获取所有列名、列值
            for (int i = 1; i < columnCount + 1; i++) {

                //列值
                Object value = resultSet.getObject(i);

                //列名
                String columnName = metaData.getColumnName(i);

                //列名：下划线命名（数据库）-->小驼峰命名（JavaBean）
                if (undertScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                }

                //一行的数据封装成t对象
                BeanUtils.setProperty(t, columnName, value);
            }

            //添加一行数据到result集合
            result.add(t);
        }

        resultSet.close();
        preparedStatement.close();

        //返回结果集合
        return result;
    }


    public static void main(String[] args) throws Exception {
        //测试Jdbc工具类
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        System.out.println(JdbcUtil.querySql("select * from GMALL2021_REALTIME.DIM_BASE_TRADEMARK where id ='1'", connection, JSONObject.class, true));

        connection.close();
    }
}
