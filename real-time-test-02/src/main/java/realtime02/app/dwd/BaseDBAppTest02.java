package realtime02.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import realtime02.bean.TableProcess;
import realtime02.func.MydeserTest02;
import realtime02.func.TableProcessFunc;
import realtime02.utils.MykafkaUtilTest02;

public class BaseDBAppTest02 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        String dbSourceTopic = "ods_base_db";

        String grouId = "base-db-app-test02";

        DataStreamSource<String> kafkaDS = env.addSource(MykafkaUtilTest02.getKafkaSource(dbSourceTopic, grouId));

        //转为JSON对象，脏数据放到侧输出流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);

                    out.collect(jsonObject);
                } catch (Exception e) {

                    ctx.output(new OutputTag<String>("脏数据") {
                    }, value);
                }
            }
        });


        //使用FlinkCDC获取Mysql配置表信息
        DataStreamSource<String> tableProcessDS = env.addSource(MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_realtime_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MydeserTest02())
                .build());


        //把配置流作为广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-stats", String.class, TableProcess.class);
        BroadcastStream<String> broadcastDS = tableProcessDS.broadcast(mapStateDescriptor);

        //主流和广播流连接
        BroadcastConnectedStream<JSONObject, String> connect = jsonObjDS.connect(broadcastDS);

        //动态分流
        OutputTag<String> hbaseOutput = new OutputTag<String>("hbaseOutput") {
        };

        SingleOutputStreamOperator<String> process = connect.process(new TableProcessFunc(mapStateDescriptor, hbaseOutput));

        process.print("kafka>>>>>>>>>>>>>>>>>");

        DataStream<String> hbaseDS = process.getSideOutput(hbaseOutput);

        hbaseDS.print("hbase>>>>>>>>>>>>>>>>");

        env.execute();

    }
}
