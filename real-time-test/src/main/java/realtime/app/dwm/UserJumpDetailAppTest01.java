package realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import realtime.utils.MyKafkaUtilTest01;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class UserJumpDetailAppTest01 {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 2.读取kafka数据 dwd_page_log
        String topic = "dwd_page_log";

        String groupId = "user-jump-detail-app-test01";

        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtilTest01.getKafkaSource(topic, groupId).setStartFromEarliest());

        //TODO 3.转为JSON对象
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
        });

        //生成WaterMark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        //TODO 4.按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWmDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });


        //TODO 5.定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {

                        //筛选出用户上一跳为null的信息
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                })
                .next("next")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {

                        //上一跳为null
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                })
                .within(Time.seconds(10));

        //TODO 6.将模式序列作用于流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        //TODO 7.提取命中事件、超时事件
        OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("time-out") {
        };

        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(outputTag, new PatternTimeoutFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp) throws Exception {

                //提取超时事件
                return pattern.get("start").get(0);
            }
        }, new PatternSelectFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject select(Map<String, List<JSONObject>> pattern) throws Exception {

                //提取命中事件
                return pattern.get("start").get(0);
            }
        });

        DataStream<JSONObject> timeoutDS = selectDS.getSideOutput(outputTag);

        selectDS.union(timeoutDS).print();


        //TODO 9.执行
        env.execute();

    }
}
