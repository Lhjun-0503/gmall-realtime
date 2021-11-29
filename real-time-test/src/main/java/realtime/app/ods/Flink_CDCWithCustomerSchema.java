package realtime.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import realtime.func.MyFlinkCDC_Deser;
import realtime.utils.MyKafkaUtilTest01;

public class Flink_CDCWithCustomerSchema {

    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.创建Flink-MySQL-CDC的Source
        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_realtime")
                .startupOptions(StartupOptions.latest())
                .deserializer(new MyFlinkCDC_Deser())
                .build();

        //3.使用CDC Source从MySQL读取数据
        DataStreamSource<String> mysqlDS = env.addSource(mysqlSource);

        //4.将数据发送至Kafka
        mysqlDS.addSink(MyKafkaUtilTest01.getKafkaSink("ods_base_db"));

        mysqlDS.print();

        //5.执行任务
        env.execute();
    }
}
