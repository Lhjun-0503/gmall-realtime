package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.VisitorStats;
import com.atguigu.gmall.realtime.utils.ClickhouseUtil;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 2.读取kafka数据 dwd_page_log  dwm_unique_visit  dwm_user_jump_detail
        String groupId = "visitor-stats-app";
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitorSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        DataStreamSource<String> pageDS = env.addSource(MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId)/*.setStartFromEarliest()*/);
        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getKafkaSource(uniqueVisitorSourceTopic, groupId)/*.setStartFromEarliest()*/);
        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId)/*.setStartFromEarliest()*/);


        //TODO 3.将各个流转为JavaBean
        //3.1处理pageDS -> pv 进入页面数  连续访问时长
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithPvUvDT = pageDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String value) throws Exception {
                //数据转为JSON对象
                JSONObject jsonObject = JSON.parseObject(value);

                //取出common公共字段
                JSONObject common = jsonObject.getJSONObject("common");

                //取出上一次页面信息
                String lastPage = jsonObject.getJSONObject("page").getString("last_page_id");

                //进入次数
                Long sv = 0L;
                if (lastPage == null || lastPage.length() <= 0) {
                    sv = 1L;
                }

                //封装统一格式对象并返回
                return new VisitorStats("",
                        "",
                        common.getString("vc"),
                        common.getString("ch"),
                        common.getString("ar"),
                        common.getString("is_new"),
                        0L,
                        1L,
                        sv,
                        0L,
                        jsonObject.getJSONObject("page").getLong("during_time"),
                        jsonObject.getLong("ts"));
            }
        });

        //3.2处理uvDS  -> uv
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUv = uvDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String value) throws Exception {

                //将数据转为JSON对象
                JSONObject jsonObject = JSON.parseObject(value);

                //取出common公共字段
                JSONObject common = jsonObject.getJSONObject("common");

                //封装JavaBean并返回
                return new VisitorStats("",
                        "",
                        common.getString("vc"),
                        common.getString("ch"),
                        common.getString("ar"),
                        common.getString("is_new"),
                        1L,
                        0L,
                        0L,
                        0L,
                        0L,
                        jsonObject.getLong("ts"));
            }
        });

        //3.3处理ujDS ->  UserJump
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUj = ujDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String value) throws Exception {
                //将数据转为JSON对象
                JSONObject jsonObject = JSON.parseObject(value);

                //提取common公共字段
                JSONObject common = jsonObject.getJSONObject("common");

                //封装JavaBean并返回
                return new VisitorStats("",
                        "",
                        common.getString("vc"),
                        common.getString("ch"),
                        common.getString("ar"),
                        common.getString("is_new"),
                        0L,
                        0L,
                        0L,
                        1L,
                        0L,
                        jsonObject.getLong("ts"));
            }
        });


        //TODO 4.Union各个流，并提取时间戳生成Watermark
        SingleOutputStreamOperator<VisitorStats> unionDS = visitorStatsWithPvUvDT.union(visitorStatsWithUv, visitorStatsWithUj)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));


        //TODO 5.按照维度信息分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = unionDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<>(value.getVc(),
                        value.getCh(),
                        value.getAr(),
                        value.getIs_new());
            }
        });

        //TODO 6.聚合
        SingleOutputStreamOperator<VisitorStats> result = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {

                        value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                        value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                        value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                        value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());


                        return value1;
                    }
                }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {

                        //提取数据
                        VisitorStats visitorStats = input.iterator().next();

                        //补充窗口时间字段
                        long start = window.getStart();
                        long end = window.getEnd();

                        //开始统计时间
                        String stt = DateTimeUtil.toYMDhms(new Date(start));
                        //统计结束时间
                        String edt = DateTimeUtil.toYMDhms(new Date(end));

                        visitorStats.setStt(stt);
                        visitorStats.setEdt(edt);

                        out.collect(visitorStats);

                    }
                });


        //打印测试
        result.map(new MapFunction<VisitorStats, String>() {
            @Override
            public String map(VisitorStats value) throws Exception {
                return JSONObject.toJSONString(value);
            }
        }).print();


        //TODO 7.将数据写出到ClickHouse
        result.addSink(ClickhouseUtil.getSinkFunc("insert into visitor_stats_210625 values(?,?,?,?,?,?,?,?,?,?,?,?)"));


        //TODO 8.执行任务
        env.execute();
    }
}
