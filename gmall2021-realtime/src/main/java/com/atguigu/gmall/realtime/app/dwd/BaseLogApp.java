package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

//数据流向   Web/App  ->  Nginx  ->  日志服务器  ->  Kafka(ods)  ->  FlinkApp  ->  Kafka(dwd)
//程序       jar包    ->  Nginx  ->  logger.sh  ->  kafka(zk)   ->  BaseLogApp  ->  kafka(zk)

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境  设置并行度  开启CK  设置状态后端（HDFS）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //为kafka主题的分区数
        env.setParallelism(1);

        /*//1.1设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));

        //1.2开启CK
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);*/

        //修改用户名
        //System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 2.读取kafka_ods_base_log主题数据
        String topic = "ods_base_log";

        String groupId = "ods_dwd_base_log_app";

        //创建KafkaSource
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);

        //从最早的数据开始消费
        //kafkaSource.setStartFromEarliest();

        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);


        //TODO 3.将每行数据转为JsonObject，并过滤脏数据
        OutputTag<String> dirtyOutputTag = new OutputTag<String>("Dirty") {
        };

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);

                    out.collect(jsonObject);
                } catch (Exception e) {

                    //脏数据写入侧输出流
                    ctx.output(dirtyOutputTag, value);
                }
            }
        });

        //脏数据  非JSON
        DataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyOutputTag);

        //打印测试
        dirtyDS.print("dirty>>>>>>>>>>>");


        //TODO 4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });


        //TODO 5.使用状态做新老客户校验
        SingleOutputStreamOperator<JSONObject> jsonWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            //声明状态表示用户已访问
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                //取出新用户标记
                String isNew = value.getJSONObject("common").getString("is_new");

                //是新用户，则进行校验
                if ("1".equals(isNew)) {
                    //获取状态
                    String flag = valueState.value();

                    //判断状态数据是否为null
                    if (flag != null) {
                        //不为null，说明用户已访问过，修复
                        value.getJSONObject("common").put("is_new", 0);

                    } else {
                        //更新状态，表示用户已访问
                        valueState.update("0");
                    }
                }

                return value;

            }
        });


        //TODO 6.页面日志输出到主流,启动日志输出到启动侧输出流,曝光日志输出到曝光日志侧输出流

        //page  （可能包含display）

        //start
        OutputTag<String> startOutputTag = new OutputTag<String>("start") {
        };

        OutputTag<String> displayOutputTag = new OutputTag<String>("display") {
        };

        SingleOutputStreamOperator<String> pageDS = jsonWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {

                //提取start字段
                String startStr = value.getString("start");

                //判断是否为start启动日志
                if (startStr != null && startStr.length() > 2) {

                    //将启动日志写到侧输出流
                    ctx.output(startOutputTag, value.toJSONString());
                } else {

                    //是page页面日志,页面日志写到主流
                    out.collect(value.toJSONString());

                    //判断是否有display曝光日志
                    JSONArray displays = value.getJSONArray("displays");

                    if (displays != null && displays.size() > 0) {
                        //有曝光数据

                        //遍历曝光数组，写到侧输出流
                        for (int i = 0; i < displays.size(); i++) {

                            //取出单条曝光日志
                            JSONObject displayJson = displays.getJSONObject(i);

                            //拼接上page_id
                            displayJson.put("page_id", value.getJSONObject("page").getString("page_id"));

                            //写到到侧输出流
                            ctx.output(displayOutputTag, displayJson.toJSONString());
                        }
                    }

                }
            }
        });


        //TODO 7.将三个流的数据写入对应的kafka主题
        DataStream<String> startDS = pageDS.getSideOutput(startOutputTag);

        DataStream<String> displayDS = pageDS.getSideOutput(displayOutputTag);

        pageDS.addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        startDS.addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));


        //打印测试
        pageDS.print("Page>>>>>>>>>>>>>>>>>>>>>>>");
        startDS.print("Start>>>>>>>>>>>>>>>>>>>>>");
        displayDS.print("Display>>>>>>>>>>>>>>>>>");

        //执行任务
        env.execute();

    }
}
