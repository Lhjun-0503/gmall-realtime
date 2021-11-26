package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;
import java.util.Date;

public class UniqueVisitAppTest01 {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 2.读取kafka数据
        String pageViewTopic = "dwd_page_log";
        String groupId = "unique-visit-app-test01";

        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(pageViewTopic, groupId).setStartFromEarliest());

        //TODO 3.转JSON
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSONObject.parseObject(value);
            }
        });


        //TODO 4.按MID分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

        //TODO 5.按状态去重
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            //定义状态保存用户登陆时间
            private ValueState<String> visitDTState;

            private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态
                ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("visit-state", String.class);

                //给状态设置24小时过期，并且在状态被创建或者被修改时，过期的倒计时要被重置为24小时
                descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.hours(24)).updateTtlOnCreateAndWrite().build());

                visitDTState = getRuntimeContext().getState(descriptor);

            }

            @Override
            public boolean filter(JSONObject value) throws Exception {

                //取出上次访问页面
                String lastPageId = value.getJSONObject("page").getString("last_page_id");


                //筛选出用户首次访问的数据
                if (lastPageId == null) {
                    //取出用户当前访问时间
                    Long ts = value.getLong("ts");
                    String curDate = sdf.format(new Date(ts));

                    //取出状态时间
                    String visitDT = visitDTState.value();

                    if (visitDT == null || !visitDT.equals(curDate)) {

                        //更新状态
                        visitDTState.update(curDate);

                        return true;
                    }

                }
                return false;
            }
        });

        filterDS.print();


        env.execute();
    }
}
