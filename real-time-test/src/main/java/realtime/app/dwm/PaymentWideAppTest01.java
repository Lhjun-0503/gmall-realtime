package realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import realtime.bean.OrderWide;
import realtime.bean.PaymentInfo;
import realtime.bean.PaymentWide;
import realtime.func.DimAsyncFunctionTest01;
import realtime.utils.MyKafkaUtilTest01;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;

public class PaymentWideAppTest01 {
    public static void main(String[] args) {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 2.读取Kafka数据 dwd_payment_info dwm_order_wide
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";

        String groupId = "payment-wide-app-test01";

        //支付表
        DataStreamSource<String> paymentInfoDS = env.addSource(MyKafkaUtilTest01.getKafkaSource(paymentInfoSourceTopic, groupId));

        //订单宽表
        DataStreamSource<String> orderWideDS = env.addSource(MyKafkaUtilTest01.getKafkaSource(orderWideSourceTopic, groupId));

        //TODO 3.转为JavaBean  生成WaterMark
        //支付
        SingleOutputStreamOperator<PaymentInfo> paymentWithWmDS = paymentInfoDS.map(new MapFunction<String, PaymentInfo>() {
            @Override
            public PaymentInfo map(String value) throws Exception {
                return JSONObject.parseObject(value, PaymentInfo.class);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                    @Override
                    public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
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

        //订单
        SingleOutputStreamOperator<OrderWide> orderWideWithWmDS = orderWideDS.map(new MapFunction<String, OrderWide>() {
            @Override
            public OrderWide map(String value) throws Exception {
                return JSONObject.parseObject(value, OrderWide.class);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
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

        //TODO 4.双流join
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentWithWmDS.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideWithWmDS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(0))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        //TODO 5.关联维度数据


        //TODO 6.写入kafka
    }
}
