package realtime.func;

import com.alibaba.fastjson.JSONObject;
import realtime.common.GmallConfig;
import realtime.utils.JdbcUtil;
import realtime.utils.RedisUtil;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class DimUtilTest01 {

    /**
     * @param connection Phoenix连接
     * @param table      维度表
     * @param key        要查询的主键的值
     * @return
     * @throws InvocationTargetException
     * @throws SQLException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public static JSONObject getDimInfo(Connection connection, String table, String key) throws InvocationTargetException, SQLException, InstantiationException, IllegalAccessException {

        //获取redis连接
        Jedis jedis = RedisUtil.getJedis();

        //先查询redis缓存
        String redisKey = table + ":" + key;

        String jsonStr = jedis.get(redisKey);

        //如果redis中存在维度信息
        if (jsonStr != null) {

            //重置redisKey的过期时间
            jedis.expire(redisKey, 24 * 60 * 60);

            //关闭连接
            jedis.close();

            return JSONObject.parseObject(jsonStr);
        }

        //redis中不存在，查询Phoenix
        //封装查询语句
        String sql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + table + " where id='" + key + "'";

        //调用JdbcUtil
        List<JSONObject> list = JdbcUtil.querySql(sql, connection, JSONObject.class, false);

        //获取查询结果
        JSONObject dimInfo = list.get(0);

        //把查询结果存入redis
        jedis.set(redisKey, dimInfo.toJSONString());

        //设置redisKey的过期时间
        jedis.expire(redisKey, 24 * 60 * 60);

        //关闭redis连接
        jedis.close();

        //返回维度信息
        return dimInfo;
    }

    //删除redis的数据
    public static void delRedisDim(String table, String key) {
        //获取redis连接
        Jedis jedis = RedisUtil.getJedis();

        String redisKey = table + ":" + key;

        jedis.del(redisKey);

        System.out.println("已删除redisKey>>>>>>>" + redisKey);

        //关闭连接
        jedis.close();

    }
}
