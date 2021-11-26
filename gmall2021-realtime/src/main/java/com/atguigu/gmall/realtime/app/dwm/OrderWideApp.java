package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.OrderDetail;
import com.atguigu.gmall.realtime.bean.OrderInfo;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;
import java.util.logging.SimpleFormatter;

public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //1.1设置状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));

        //1.2开启ck
        //env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);

        //env.getCheckpointConfig().setCheckpointTimeout(60000L);

        //TODO 2.读取kafka订单明细
        String orderInfoSourceTopic = "dwd_order_info";

        String orderDetailSourceTopic = "dwd_order_detail";

        String orderWideSinkTopic = "dwm_order_wide";

        String groupId = "order_wide_group";

        FlinkKafkaConsumer<String> orderInfoKafkaSource = MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId);

        //从头开始消费
        orderInfoKafkaSource.setStartFromEarliest();

        //订单信息
        DataStreamSource<String> orderInfoKafkaDS = env.addSource(orderInfoKafkaSource);


        FlinkKafkaConsumer<String> orderDetailSource = MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId);

        //从头开始消费
        //orderDetailSource.setStartFromEarliest();

        //订单明细
        DataStreamSource<String> orderDetailKafkaDS = env.addSource(orderDetailSource);


        //TODO 3.将每行数据转为JavaBean，提取时间戳生成WaterMark
        //订单
        SingleOutputStreamOperator<OrderInfo> orderInfoWithWMDS = orderInfoKafkaDS.map(new MapFunction<String, OrderInfo>() {
            @Override
            public OrderInfo map(String value) throws Exception {
                OrderInfo orderInfo = JSON.parseObject(value, OrderInfo.class);


                //yyyy-MM-dd HH:mm:ss
                String create_time = orderInfo.getCreate_time();

                String[] dateHourArr = create_time.split(" ");

                //补全JavaBean字段
                orderInfo.setCreate_date(dateHourArr[0]);
                orderInfo.setCreate_hour(dateHourArr[1].split(":")[0]);

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                long ts = sdf.parse(create_time).getTime();

                //补全JavaBean字段
                orderInfo.setCreate_ts(ts);

                return orderInfo;
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));

        //订单明细
        SingleOutputStreamOperator<OrderDetail> orderDetailWithWMDS = orderDetailKafkaDS.map(new MapFunction<String, OrderDetail>() {
            @Override
            public OrderDetail map(String value) throws Exception {
                OrderDetail orderDetail = JSON.parseObject(value, OrderDetail.class);

                //yyyy-MM-dd HH:mm:ss
                String create_time = orderDetail.getCreate_time();

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                long ts = sdf.parse(create_time).getTime();

                //补全JavaBean字段
                orderDetail.setCreate_ts(ts);

                return orderDetail;
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));


        //TODO 4.将两个流进行join
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoWithWMDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailWithWMDS.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });


        //TODO 5.关联维度信息
        //TODO 5.1 关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(orderWideDS,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getId(OrderWide orderWide) {

                        //获取要关联的主键
                        return orderWide.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws Exception {

                        //查询维度表数据
                        String birthday = dimInfo.getString("BIRTHDAY");
                        String gender = dimInfo.getString("GENDER");

                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

                        //获取当前时间
                        long ts = System.currentTimeMillis();

                        long time = ts;
                        try {
                            //获取用户生日时间对应的时间戳
                            time = sdf.parse(birthday).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }

                        //当前时间-用户生日  得到用户年龄
                        long age = (ts - time) / (1000L * 60 * 60 * 24 * 365);

                        //将性别和年龄补充到OrderWide
                        orderWide.setUser_gender(gender);
                        orderWide.setUser_age(Integer.parseInt(age + ""));

                    }
                },
                100,  //方法的最后两个参数10, TimeUnit.SECONDS ，标识次异步查询最多执行10秒，否则会报超时异常
                TimeUnit.SECONDS);


        //TODO 5.2 关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {

                    //获取要关联的主键
                    @Override
                    public String getId(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws Exception {

                        //查询维度表数据
                        String name = dimInfo.getString("NAME");
                        String area_code = dimInfo.getString("AREA_CODE");
                        String iso_code = dimInfo.getString("ISO_CODE");
                        String iso_3166_2 = dimInfo.getString("ISO_3166_2");

                        //将地区数据补充到orderWide
                        orderWide.setProvince_name(name);
                        orderWide.setProvince_area_code(area_code);
                        orderWide.setProvince_iso_code(iso_code);
                        orderWide.setProvince_3166_2_code(iso_3166_2);


                    }
                }, 100,
                TimeUnit.SECONDS);

        //TODO 5.3 关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(orderWideWithProvinceDS,
                new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {

                    //获取要关联的主键
                    @Override
                    public String getId(OrderWide orderWide) {
                        return orderWide.getSku_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws Exception {

                        //获取维度数据
                        String sku_name = dimInfo.getString("SKU_NAME");
                        Long category3_id = dimInfo.getLong("CATEGORY3_ID");
                        Long spu_id = dimInfo.getLong("SPU_ID");
                        Long tm_id = dimInfo.getLong("TM_ID");

                        //将SKU数据补充到orderWide
                        orderWide.setSku_name(sku_name);
                        orderWide.setCategory3_id(category3_id);
                        orderWide.setSpu_id(spu_id);
                        orderWide.setTm_id(tm_id);
                    }
                }, 100,
                TimeUnit.SECONDS);

        //TODO 5.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(orderWideWithSkuDS,
                new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {

                    //获取要关联的主键
                    @Override
                    public String getId(OrderWide orderWide) {
                        return orderWide.getSpu_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws Exception {

                        //获取维度数据
                        String spu_name = dimInfo.getString("SPU_NAME");

                        //把SPU数据补充到orderWide
                        orderWide.setSpu_name(spu_name);
                    }
                }, 100,
                TimeUnit.SECONDS);

        //TODO 5.5 关联TradeMark维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(orderWideWithSpuDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {

                    //获取要关联的主键
                    @Override
                    public String getId(OrderWide orderWide) {
                        return orderWide.getTm_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws Exception {

                        orderWide.setTm_name(dimInfo.getString("TM_NAME"));
                    }
                }, 100,
                TimeUnit.SECONDS);

        //TODO 5.6 关联Category维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(orderWideWithTmDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {

                    //获取要关联的主键
                    @Override
                    public String getId(OrderWide orderWide) {
                        return orderWide.getCategory3_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws Exception {

                        orderWide.setCategory3_name(dimInfo.getString("NAME"));
                    }
                }, 100,
                TimeUnit.SECONDS);


        //打印测试
        orderWideWithCategory3DS.print();

        //TODO 6.数据写入kafka
        orderWideWithCategory3DS.map(new MapFunction<OrderWide, String>() {
            @Override
            public String map(OrderWide value) throws Exception {
                return JSON.toJSONString(value);
            }
        }).addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));




        //执行
        env.execute();





    }
}
