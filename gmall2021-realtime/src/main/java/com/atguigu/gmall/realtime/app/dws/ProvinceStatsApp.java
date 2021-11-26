package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.ProvinceStats;
import com.atguigu.gmall.realtime.utils.ClickhouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProvinceStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //TODO 2.使用DDL的方式创建表，提取时间戳字段生成WaterMark
        String topic = "dwm_order_wide";
        String groupId = "province-stats-app";

        tableEnv.executeSql("create table order_wide(" +
                "order_id bigint," +
                "province_id bigint," +
                "province_name string," +
                "province_area_code string," +
                "province_iso_code string," +
                "province_3166_2_code string," +
                "total_amount decimal," +
                "create_time string," +
                "rowtime as TO_TIMESTAMP(create_time)," +
                "WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND " +
                ") WITH(" + MyKafkaUtil.getKafkaDDL(topic, groupId) + ")"
        );


        //TODO 3.执行查询  开窗、分组、聚合
        Table tableResult = tableEnv.sqlQuery("select  " +
                "    DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') stt,  " +
                "    DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') edt,  " +
                "    province_id,  " +
                "    province_name,  " +
                "    province_area_code,  " +
                "    province_iso_code,  " +
                "    province_3166_2_code,  " +
                "    sum(total_amount) order_amount,  " +
                "    count(distinct order_id) order_count,  " +
                "    UNIX_TIMESTAMP()*1000 ts  " +
                "from order_wide  " +
                "group by   " +
                "    province_id,  " +
                "    province_name,  " +
                "    province_area_code,  " +
                "    province_iso_code,  " +
                "    province_3166_2_code,  " +
                "    TUMBLE(rowtime, INTERVAL '10' SECOND)");

        //TODO 4.将查询结果的动态表转为流
        DataStream<ProvinceStats> provinceStatsDataStream = tableEnv.toAppendStream(tableResult, ProvinceStats.class);

        //打印测试
        provinceStatsDataStream.map(JSON::toJSONString).print();

        //TODO 5.将数据写入ClickHouse
        provinceStatsDataStream.addSink(ClickhouseUtil.getSinkFunc("insert into province_stats_210625 values(?,?,?,?,?,?,?,?,?,?)"));

        //TODO 6.启动
        env.execute();

    }
}
