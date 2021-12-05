package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.func.DimSink;
import com.atguigu.gmall.realtime.func.MyFlinkCDC_Deser;
import com.atguigu.gmall.realtime.func.TableProcessFunction;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nullable;

//数据流  Web/App -> Nginx -> SpringBoot -> Mysql(Binlog) -> Flink     -> Kafka -> Flink -> Kafka-Phoenix
//进程               MockDB              -> Mysql(Binlog) -> FlinkCDC  -> Kafka -> BaseDbApp -> Kafka-Phoenix

public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        /*//设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));

        //开启CK
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);*/

        //TODO 2.读取kafka数据
        String topic = "ods_base_db";
        String groupId = "ods_db_group";

        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);

        //从最早的数据开始消费
        //kafkaSource.setStartFromEarliest();

        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);


        //TODO 3.将每行数据转为JSON对象  并过滤脏数据
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


        //TODO 5.用Flink-CDC-Mysql抓取Mysql的配置表信息  获取配置流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_realtime_process")
                .tableList("gmall_realtime_process.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyFlinkCDC_Deser())
                .build();

        DataStreamSource<String> tableProcessDS = env.addSource(sourceFunction);


        //TODO 6.将配置信息流作为广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("table-process-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessDS.broadcast(mapStateDescriptor);



        //TODO 7.将主流和广播流进行连接
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterDS.connect(broadcastStream);

        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE) {
        };



        //主流比广播流先到  会存在主流找不到对应的配置流中的维度信息，丢失部分维度数据
        //第一次运行实时任务，先启动BaseDBApp，先拿到配置流信息，主流没有数据，再启动FlinkCDC抓取业务数据获得主流

        //如果离线、实时共用一个框架，已经有历史的业务数据，主流中已经有数据
        //动态分流的时候，在TableProcessFunc的open方法中查一次Mysql中的配置表信息，以Map形式存储，主流如果找不到对应的配置流的信息，再去这个Map查询，防止丢失维度信息


        //TODO 8.动态分流
        SingleOutputStreamOperator<JSONObject> kafkaJsonDS = connectedStream.process(new TableProcessFunction(hbaseTag,mapStateDescriptor));

        DataStream<JSONObject> hbaseJsonDS = kafkaJsonDS.getSideOutput(hbaseTag);


        //有多张表要写入HBase，每张表的字段数量不同，不方便使用JDBCSink，使用自定义Sink


        //TODO 维度表写入hbase
        hbaseJsonDS.addSink(new DimSink());


        //获取kafkaSink
        FlinkKafkaProducer<JSONObject> kafkaSinkBySchema = MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                System.out.println("开始序列化kafka数据");
            }

            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<byte[], byte[]>(element.getString("sink_table"), element.getString("after").getBytes());
            }
        });


        //TODO 事实表写入kafka
        kafkaJsonDS.addSink(kafkaSinkBySchema);



        //打印测试
        kafkaJsonDS.print("kafka>>>>>>>>>");
        hbaseJsonDS.print("hbase>>>>>>>>>");



        //7.执行任务
        env.execute();
    }
}
