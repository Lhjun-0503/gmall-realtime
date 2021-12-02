package realtime.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import realtime.func.MyFlinkCDC_DeserTest01;
import realtime.utils.MyKafkaUtilTest01;

public class Flink_CDCWithCustomerSchemaTest01 {
    public static void main(String[] args) throws Exception {


        //TODO 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 创建FlinkCDC MySQLSource
        DataStreamSource<String> mysqlDS = env.addSource(MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_realtime")
                .startupOptions(StartupOptions.latest())
                .deserializer(new MyFlinkCDC_DeserTest01())
                .build());


        //将数据发送至Kafka
        mysqlDS.addSink(MyKafkaUtilTest01.getKafkaSink("ods_base_db"));

        mysqlDS.print();

        env.execute("Flink_CDCWithCustomerSchemaTest01");
    }
}
