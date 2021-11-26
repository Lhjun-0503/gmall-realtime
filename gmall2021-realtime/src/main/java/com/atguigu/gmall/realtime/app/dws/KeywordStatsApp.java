package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.bean.KeywordStats;
import com.atguigu.gmall.realtime.func.KeywordUDTF;
import com.atguigu.gmall.realtime.utils.ClickhouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import ru.yandex.clickhouse.ClickHouseUtil;

public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.使用DDL方式读取kafka数据创建表
        String topic = "dwd_page_log";
        String groupId = "keyword-stats-app";

        tableEnv.executeSql("create table page_view( " +
                "common Map<String,String>, " +
                "page Map<String,String>, " +
                "ts bigint, " +
                "rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                "WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND" +
                ")with(" + MyKafkaUtil.getKafkaDDL(topic, groupId) + ")"
        );

        //TODO 3.过滤数据
        Table fullWordTable = tableEnv.sqlQuery("" +
                "select " +
                "   page['item'] fullWord, " +
                "   rowtime " +
                "from page_view " +
                "where page['item_type'] = 'keyword' " +
                "and page['last_page_id'] = 'search' " +
                "and page['item'] is not null"
        );


        //TODO 4.注册UDTF函数并进行分词处理
        tableEnv.createTemporarySystemFunction("SplitFunction", KeywordUDTF.class);

        Table splitWordTable = tableEnv.sqlQuery("SELECT word,rowtime FROM " + fullWordTable + ", LATERAL TABLE(SplitFunction(fullWord))");


        //TODO 5.开窗、分组、聚合
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "   'search' source, " +
                "   DATE_FORMAT(TUMBLE_START(rowtime,INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') stt, " +
                "   DATE_FORMAT(TUMBLE_END(rowtime,INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') edt, " +
                "   word keyword, " +
                "   count(*) ct, " +
                "   UNIX_TIMESTAMP()*1000 ts " +
                "from " + splitWordTable +
                " group by word, " +
                " TUMBLE(rowtime, INTERVAL '10' SECOND)"
        );


        //TODO 6.转为流并写入ClickHouse
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(resultTable, KeywordStats.class);

        //打印测试
        keywordStatsDataStream.print();

        //keywordStatsDataStream.addSink(ClickhouseUtil.getSinkFunc("insert into keyword_stats_210625 values(?,?,?,?,?,?)"));
        keywordStatsDataStream.addSink(ClickhouseUtil.getSinkFunc("insert into keyword_stats_210625(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));
        //TODO 7.执行
        env.execute();
    }
}
