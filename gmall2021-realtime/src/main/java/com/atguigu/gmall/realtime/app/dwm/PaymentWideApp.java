package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.PaymentInfo;
import com.atguigu.gmall.realtime.bean.PaymentWide;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.eclipse.jetty.util.ajax.JSON;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;

public class PaymentWideApp {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 2.读取kafka的dwm_order_wide  dwd_payment_info
        String groupId = "payment-wide-app";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentInfoSourceTopic = "dwd_payment_info";

        String paymentWideSinkTopic = "dwm_payment_wide";

        DataStreamSource<String> orderWideStrDS = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId)/*.setStartFromEarliest()*/);

        DataStreamSource<String> paymentInfoStrDS = env.addSource(MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId)/*.setStartFromEarliest()*/);

        //TODO 3.将数据转为JavaBean  并提取数据中的时间戳生成WaterMark
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideStrDS.map(new MapFunction<String, OrderWide>() {
            @Override
            public OrderWide map(String value) throws Exception {
                return JSONObject.parseObject(value, OrderWide.class);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
            @Override
            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                String create_time = element.getCreate_time();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                try {
                    return sdf.parse(create_time).getTime();
                } catch (ParseException e) {
                    e.printStackTrace();
                    return recordTimestamp;
                }
            }
        }));

        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentInfoStrDS.map(new MapFunction<String, PaymentInfo>() {
            @Override
            public PaymentInfo map(String value) throws Exception {
                return JSONObject.parseObject(value, PaymentInfo.class);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
            @Override
            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                String create_time = element.getCreate_time();
                SimpleDateFormat sdf = new SimpleDateFormat(("yyyy-MM-dd HH:mm:ss"));

                try {
                    return sdf.parse(create_time).getTime();
                } catch (ParseException e) {
                    e.printStackTrace();
                    return recordTimestamp;
                }
            }
        }));

        //TODO 4.双流Join
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(0))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });



        //TODO 5.将数据写入Kafka  dwm_payment_wide
        paymentWideDS.map(new MapFunction<PaymentWide, String>() {
            @Override
            public String map(PaymentWide value) throws Exception {
                return JSONObject.toJSONString(value);

            }
        }).addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));

        //打印测试
        paymentWideDS.map(new MapFunction<PaymentWide, String>() {
            @Override
            public String map(PaymentWide value) throws Exception {
                return JSONObject.toJSONString(value);

            }
        }).print();

        //TODO 6.执行任务
        env.execute();
    }
}
