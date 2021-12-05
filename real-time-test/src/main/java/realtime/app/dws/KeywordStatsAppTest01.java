package realtime.app.dws;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import realtime.bean.KeywordStats;
import realtime.func.KeywordUDTFTest01;
import realtime.utils.ClickHouseUtilTest01;
import realtime.utils.MyKafkaUtilTest01;


public class KeywordStatsAppTest01 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String topic = "dwd_page_log";

        String groupId = "keyword-stats-app-test01";

        //创建动态表
        tableEnv.executeSql("create table page_log (" +
                " page Map<String,String>, " +
                " ts BIGINT, " +
                " rt as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                " WATERMARK FOR rt AS rt - INTERVAL '1' SECOND " +
                " ) WITH (" + MyKafkaUtilTest01.getKafkaTable(topic, groupId) + ")");



        //过滤出搜索的日志
        Table filterTable = tableEnv.sqlQuery("select " +
                " page['item'] key_word, " +
                " rt " +
                " from page_log " +
                " where page['item_type'] = 'keyword' " +
                " and page['item'] is not null");


        //创建临时视图
        tableEnv.createTemporaryView("filter_table",filterTable);

        //注册自定义的UDTF切词函数
        tableEnv.createTemporarySystemFunction("keywordUDTF", KeywordUDTFTest01.class);

        //切分关键词
        Table wordTable = tableEnv.sqlQuery("select word, rt from filter_table,LATERAL TABLE(keywordUDTF(key_word))");

        //创建临时视图
        tableEnv.createTemporaryView("word_table",wordTable);

        //分组  开窗  聚合
        Table result = tableEnv.sqlQuery("select " +
                "'search' source," +
                "DATE_FORMAT(TUMBLE_START(rt,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "DATE_FORMAT(TUMBLE_END(rt,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "word keyword, " +
                "count(*) ct, " +
                "UNIX_TIMESTAMP()*1000 ts " +
                "from word_table " +
                "group by word, " +
                "TUMBLE(rt,INTERVAL '10' SECOND)");

        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(result, KeywordStats.class);


        //打印测试
        keywordStatsDataStream.map(JSON::toJSONString).print();


        //写入clickHouse
        keywordStatsDataStream.addSink(ClickHouseUtilTest01.getSinkFunc("insert into keyword_stats_210625(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));

        env.execute();
    }
}
