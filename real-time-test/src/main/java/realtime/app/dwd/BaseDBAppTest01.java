package realtime.app.dwd;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import realtime.bean.TableProcess;
import realtime.func.DimSinkTest01;
import realtime.func.MyFlinkCDC_Deser;
import realtime.utils.MyKafkaUtilTest01;
import realtime.func.TableProcessFuncTest01;

import javax.annotation.Nullable;

public class BaseDBAppTest01 {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.读取kafka的ods_base_db
        String odsBaseDbSourceTopic = "ods_base_db";
        String groupId = "base-db-app-test01";

        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtilTest01.getKafkaSource(odsBaseDbSourceTopic, groupId);

        //kafkaSource.setStartFromEarliest();

        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);


        //TODO 3.转为JSON，并过滤脏数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("脏数据：" + value);
                }
            }
        });


        //TODO 4.过滤"delete"类型的数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return !"delete".equals(value.getString("type"));
            }
        });

        //TODO 5.使用Flink-CDC-Mysql抓取配置表信息  获取配置流
        DebeziumSourceFunction<String> mySqlSource = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_realtime_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyFlinkCDC_Deser())
                .build();

        DataStreamSource<String> processDS = env.addSource(mySqlSource);

        //TODO 6.把配置流作为广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = processDS.broadcast(mapStateDescriptor);



        //TODO 7.主流和广播流连接
        BroadcastConnectedStream<JSONObject, String> connectDS = filterDS.connect(broadcastStream);

        //TODO 8.动态分流
        OutputTag<JSONObject> hbaseOutput = new OutputTag<JSONObject>("hbase") {
        };

        SingleOutputStreamOperator<JSONObject> kafkaProcessDS = connectDS.process(new TableProcessFuncTest01(mapStateDescriptor,hbaseOutput));

        DataStream<JSONObject> hbaseDS = kafkaProcessDS.getSideOutput(hbaseOutput);


        //TODO 打印测试
        kafkaProcessDS.print("kafka>>>>>>>>>");
        hbaseDS.print("hbase>>>>>>>>>");


        //数据写入HBase
        hbaseDS.addSink(new DimSinkTest01());


        //数据写入kafka
        kafkaProcessDS.addSink(MyKafkaUtilTest01.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                System.out.println("开始序列化kafka数据  ");
            }

            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<byte[], byte[]>(element.getString("sinkTable"),element.getString("after").getBytes());
            }
        }));

        //TODO 执行
        env.execute();
    }
}
