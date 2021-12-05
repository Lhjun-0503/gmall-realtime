package realtime02.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import realtime02.func.MydeserTest02;
import realtime02.utils.MykafkaUtilTest02;

public class FlinkCDC_MydeserTest02 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //使用FlinkCDC抓取Mysql业务数据
        DataStreamSource<String> mysqlDS = env.addSource(MySQLSource.<String>builder()
                .hostname("hadoop102")
                .username("root")
                .password("123456")
                .port(3306)
                .databaseList("gmall_realtime")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MydeserTest02())
                .build());

        //打印测试
        mysqlDS.print();

        //数据写入kafka
        mysqlDS.addSink(MykafkaUtilTest02.getKafkaSink("ods_base_db"));




        env.execute();
    }
}
