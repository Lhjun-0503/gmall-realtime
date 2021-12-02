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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import realtime.bean.OrderDetail;
import realtime.bean.OrderInfo;
import realtime.bean.OrderWide;
import realtime.func.DimAsyncFunctionTest01;
import realtime.utils.MyKafkaUtilTest01;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

public class OrderWideAppTest01 {
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

        FlinkKafkaConsumer<String> orderInfoKafkaSource = MyKafkaUtilTest01.getKafkaSource(orderInfoSourceTopic, groupId);

        //从头开始消费
        //orderInfoKafkaSource.setStartFromEarliest();

        //订单信息
        DataStreamSource<String> orderInfoKafkaDS = env.addSource(orderInfoKafkaSource);


        FlinkKafkaConsumer<String> orderDetailSource = MyKafkaUtilTest01.getKafkaSource(orderDetailSourceTopic, groupId);

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
        SingleOutputStreamOperator<OrderWide> orderWideDs = orderInfoWithWMDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailWithWMDS.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        //TODO 5.关联维度信息
        //TODO 5.1 关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(orderWideDs,
                new DimAsyncFunctionTest01<OrderWide>("DIM_USER_INFO") {

                    //跟据事实数据获取要关联的维度表主键
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }

                    //关联维度信息
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {

                        System.out.println("查询到的用户维度信息：" + dimInfo);

                        String gender = dimInfo.getString("GENDER");

                        orderWide.setUser_gender(gender);

                        String birthday = dimInfo.getString("BIRTHDAY");

                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

                        long birthTS = sdf.parse(birthday).getTime();
                        long curTS = System.currentTimeMillis();

                        //用户年龄
                        long age = (curTS - birthTS) / (1000 * 60 * 60 * 24 * 365L);

                        orderWide.setUser_age((int) age);

                    }
                },
                60,
                TimeUnit.SECONDS);


        //TODO 5.2 关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS,
                new DimAsyncFunctionTest01<OrderWide>("DIM_BASE_PROVINCE") {

                    //根据事实数据获取要关联的维度表主键
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    //关联维度数据
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        System.out.println("查询到的地区维度信息：" + dimInfo);

                        orderWide.setProvince_name(dimInfo.getString("NAME"));
                        orderWide.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code("ISO_CODE");
                        orderWide.setProvince_3166_2_code("IOS_3166_2");
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 5.3 关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(orderWideWithProvinceDS,
                new DimAsyncFunctionTest01<OrderWide>("DIM_SKU_INFO") {

                    //根据事实数据获取要关联的维度表主键
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSku_id().toString();
                    }

                    //关联维度数据
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        System.out.println("查询到的sku维度信息" + dimInfo);

                        orderWide.setSpu_id(dimInfo.getLong("SPU_ID"));
                        orderWide.setTm_id(dimInfo.getLong("TM_ID"));
                        orderWide.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 5.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(orderWideWithSkuDS,
                new DimAsyncFunctionTest01<OrderWide>("DIM_SPU_INFO") {

                    //根据事实数据获取要关联的维度表主键
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSpu_id().toString();
                    }

                    //关联维度数据
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        System.out.println("查询到的spu维度信息" + dimInfo);

                        orderWide.setSpu_name(dimInfo.getString("SPU_NAME"));
                    }
                }, 60, TimeUnit.SECONDS);


        //TODO 5.5 关联TradeMark维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(orderWideWithSpuDS,
                new DimAsyncFunctionTest01<OrderWide>("DIM_BASE_TRADEMARK") {

                    //根据事实数据获取要关联的维度表主键
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getTm_id().toString();
                    }

                    //关联维度数据
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        System.out.println("查询到的tm维度信息" + dimInfo);

                        orderWide.setTm_name(dimInfo.getString("TM_NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 5.6 关联Category维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategoryDS = AsyncDataStream.unorderedWait(orderWideWithTmDS,
                new DimAsyncFunctionTest01<OrderWide>("DIM_BASE_CATEGORY3") {

                    //根据事实数据获取要关联的维度表主键
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getCategory3_id().toString();
                    }

                    //关联维度数据
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        System.out.println("查询到的category维度信息" + dimInfo);

                        orderWide.setCategory3_name(dimInfo.getString("NAME"));


                    }
                }, 60, TimeUnit.SECONDS);


        //打印测试
        orderWideWithCategoryDS.print("Category>>>>>>>>>>");

        //TODO 6.数据写入kafka
        orderWideWithCategoryDS.map(JSON::toJSONString).addSink(MyKafkaUtilTest01.getKafkaSink(orderWideSinkTopic));


        //执行
        env.execute();
    }
}
