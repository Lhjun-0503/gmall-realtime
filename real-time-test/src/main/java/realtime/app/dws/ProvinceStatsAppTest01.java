package realtime.app.dws;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import realtime.bean.ProvinceStats;
import realtime.utils.ClickHouseUtilTest01;
import realtime.utils.MyKafkaUtilTest01;

public class ProvinceStatsAppTest01 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String topic = "dwm_order_wide";

        String groupId = "province-stats-app-test01";

        tableEnv.executeSql("CREATE TABLE order_wide ( " +
                "  `order_id` BIGINT, " +
                "  `province_id` BIGINT, " +
                "  `province_name` STRING, " +
                "  `province_area_code` STRING, " +
                "  `province_iso_code` STRING, " +
                "  `province_3166_2_code` STRING, " +
                "  `split_total_amount` DECIMAL, " +
                "  `create_time` STRING, " +
                "  `rt` as TO_TIMESTAMP(create_time), " +
                "  WATERMARK FOR rt AS rt - INTERVAL '1' SECOND " +
                ") WITH (" + MyKafkaUtilTest01.getKafkaTable(topic, groupId) +
                ")");

        Table result = tableEnv.sqlQuery("select " +
                "   DATE_FORMAT(TUMBLE_START(rt,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "   DATE_FORMAT(TUMBLE_END(rt,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "   province_id, " +
                "   province_name, " +
                "   province_area_code, " +
                "   province_iso_code, " +
                "   province_3166_2_code, " +
                "   sum(split_total_amount) order_amount, " +
                "   count(distinct order_id) order_count, " +
                "   UNIX_TIMESTAMP()*1000 ts" +
                " from order_wide " +
                " group by " +
                "   province_id, " +
                "   province_name, " +
                "   province_area_code, " +
                "   province_iso_code, " +
                "   province_3166_2_code, " +
                "   TUMBLE(rt,INTERVAL '10' SECOND)"
        );

        DataStream<ProvinceStats> provinceStatsDataStreamDS = tableEnv.toAppendStream(result, ProvinceStats.class);

        provinceStatsDataStreamDS.map(JSON::toJSONString).print();

        provinceStatsDataStreamDS.addSink(ClickHouseUtilTest01.getSinkFunc("insert into province_stats_210625 values(?,?,?,?,?,?,?,?,?,?)"));

        env.execute();
    }

}
