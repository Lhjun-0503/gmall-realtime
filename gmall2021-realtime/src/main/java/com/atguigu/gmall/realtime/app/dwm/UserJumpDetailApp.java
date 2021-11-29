package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //1.1设置状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));

        //1.2开启CK
        //env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000L);

        //TODO 2.读取kafka的dwd_page_log主题数据创建流
        String sourceTopic = "dwd_page_log";

        String groupId = "userJumpDetailApp";

        String sinkTopic = "dwm_user_jump_detail";

        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);

        //kafkaSource.setStartFromEarliest();

        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);


        //TODO 3.将数据转为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {

            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);

                    out.collect(jsonObject);
                } catch (Exception e) {

                    //防止脏数据
                    ctx.output(new OutputTag<String>("dirty") {
                    }, value);
                }


            }
        })
                //设定WaterMark
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");
                            }
                        }));


        //TODO 4.按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

        //TODO 5.定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("begin")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");

                        //筛选出用户的首页访问
                        return lastPageId == null || lastPageId.length() <= 0;
                    }
                }).times(2)    //默认的使用宽松近邻
                .consecutive()  //指定使用严格近邻
                .within(Time.seconds(10));

        //TODO 6.将模式序列作用在流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        //TODO 7.提取事件和超时事件
        OutputTag<String> timeOutTag = new OutputTag<String>("TimeOut") {
        };

        SingleOutputStreamOperator<String> selectDS = patternStream.select(timeOutTag, new PatternTimeoutFunction<JSONObject, String>() {
                    @Override
                    public String timeout(Map<String, List<JSONObject>> map, long l) throws Exception {

                        //提取超时事件
                        List<JSONObject> begin = map.get("begin");
                        return begin.get(0).toJSONString();
                    }
                },
                new PatternSelectFunction<JSONObject, String>() {
                    @Override
                    public String select(Map<String, List<JSONObject>> map) throws Exception {

                        //提取匹配到的事件
                        List<JSONObject> begin = map.get("begin");

                        return begin.get(0).toJSONString();
                    }
                });



        DataStream<String> userJumpDetailDS = selectDS.getSideOutput(timeOutTag);

        DataStream<String> result = selectDS.union(userJumpDetailDS);

        //打印测试
        result.print(">>>>>>>>>>>>");

        //TODO 8.将数据写入kafka
        result.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        //执行
        env.execute();
    }
}
