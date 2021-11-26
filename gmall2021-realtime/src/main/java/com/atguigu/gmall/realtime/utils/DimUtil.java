package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * Redis:
 * 1.存什么数据？         维度数据   JsonStr
 * 2.用什么类型？         String  Set  Hash
 * 3.RedisKey的设计？     String：tableName+id  Set:tableName  Hash:tableName
 */

public class DimUtil {

    public static JSONObject getDimInfo(String tableName, String value) {

        //先查询Redis中的数据
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = tableName + ":" + value;
        String jsonStr = jedis.get(redisKey);

        if (jsonStr != null) {
            JSONObject jsonObject = JSON.parseObject(jsonStr);
            jedis.expire(redisKey, 24 * 60 * 60);
            jedis.close();
            return jsonObject;
        }

        //当Redis中不存在该数据时
        //封装SQL语句
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id='" + value + "'";

        //查询Phoenix
        List<JSONObject> queryList = PhoenixUtil.queryList(querySql, JSONObject.class, false);
        JSONObject jsonObject = queryList.get(0);

        //将数据写入Redis
        jedis.set(redisKey, jsonObject.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);
        jedis.close();

        //返回结果
        return jsonObject;


    }

    public static void delRedisDim(String tableName, String value) {

        //获取Redis连接
        Jedis jedis = RedisUtil.getJedis();

        //拼接key
        String redisKey = tableName + ":" + value;


        //执行删除操作
        jedis.del(redisKey);

        System.out.println("已删除："+redisKey);

        //释放连接
        jedis.close();
    }


    public static void main(String[] args) {

        long start = System.currentTimeMillis();
        System.out.println(getDimInfo("BASE_TRADEMARK", "13"));
        long end = System.currentTimeMillis();

        System.out.println(getDimInfo("BASE_TRADEMARK", "13"));
        long end2 = System.currentTimeMillis();

        System.out.println("第一次查询，直接查HBase：" + (end - start));
        System.out.println("第二次查询，数据已写入Redis缓存：" + (end2 - end));


    }
}
