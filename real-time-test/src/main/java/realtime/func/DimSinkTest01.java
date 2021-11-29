package realtime.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import realtime.common.GmallConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSinkTest01 extends RichSinkFunction<JSONObject> {

    //Phoenix连接
    private Connection connection;

    //获取连接
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //{"db":"","tn":"","before":"","after":"","type":"","sinkTable":""}
    @Override
    public void invoke(JSONObject jsonObject, Context context) throws Exception {

        PreparedStatement preparedStatement = null;
        try {

            JSONObject after = jsonObject.getJSONObject("after");

            String table = jsonObject.getString("sinkTable");

            String sql = genSQL(after, table);

            System.out.println("Phoenix插入数据：" + sql);

            //编译sql
            preparedStatement = connection.prepareStatement(sql);


            //如果是更新操作，先删除redis的缓存，再更新Phoenix的数据
            if ("update".equals(jsonObject.getString("type"))) {

                System.out.println("删除redisKey>>>>>" + table.toUpperCase() + ":" + after.getString("id"));

                DimUtilTest01.delRedisDim(table, after.getString("id"));

            }

            //执行
            preparedStatement.execute();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }


    }

    //插入数据sql  upsert into db.tn (id,name,sex) values('aa','dd','cc')
    private String genSQL(JSONObject after, String sinkTable) {

        //获取字段
        Set<String> columns = after.keySet();

        //使用工具类把字段集合转为字符串，按照","连接元素
        String columnsStr = StringUtils.join(columns, ",");

        //获取列值
        Collection<Object> values = after.values();

        //使用工具类把字段集合转为字符串，按照"','"连接元素  插入数据语句的值用单引号
        String valuesStr = StringUtils.join(values, "','");


        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "("
                + columnsStr + ")" + "values('" + valuesStr + "')";
    }
}
