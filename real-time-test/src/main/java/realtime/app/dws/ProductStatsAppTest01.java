package realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import realtime.bean.OrderWide;
import realtime.bean.PaymentWide;
import realtime.bean.ProductStats;
import realtime.common.GmallConstant;
import realtime.func.DimAsyncFunctionTest01;
import realtime.utils.ClickHouseUtilTest01;
import realtime.utils.DateTimeUtil;
import realtime.utils.MyKafkaUtilTest01;

import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class ProductStatsAppTest01 {

    public static void main(String[] args) throws Exception {

        //TODO 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 2.读取kafka数据
        String groupId = "product_stats_app_210625";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";
        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtilTest01.getKafkaSource(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtilTest01.getKafkaSource(orderWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> paymentWideSource = MyKafkaUtilTest01.getKafkaSource(paymentWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> favorInfoSourceSource = MyKafkaUtilTest01.getKafkaSource(favorInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> cartInfoSource = MyKafkaUtilTest01.getKafkaSource(cartInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> refundInfoSource = MyKafkaUtilTest01.getKafkaSource(refundInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> commentInfoSource = MyKafkaUtilTest01.getKafkaSource(commentInfoSourceTopic, groupId);

        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> favorInfoDStream = env.addSource(favorInfoSourceSource);
        DataStreamSource<String> orderWideDStream = env.addSource(orderWideSource);
        DataStreamSource<String> paymentWideDStream = env.addSource(paymentWideSource);
        DataStreamSource<String> cartInfoDStream = env.addSource(cartInfoSource);
        DataStreamSource<String> refundInfoDStream = env.addSource(refundInfoSource);
        DataStreamSource<String> commentInfoDStream = env.addSource(commentInfoSource);

        //TODO 3.转为JavaBean
        //3.1 商品的点击、曝光
        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplay = pageViewDStream.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String value, Collector<ProductStats> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);

                //获取页面信息
                JSONObject page = jsonObject.getJSONObject("page");

                if ("good_detail".equals(page.getString("page_id")) && "sku_id".equals(page.getString("item_type"))) {
                    out.collect(ProductStats
                            .builder()
                            .click_ct(1L)
                            .sku_id(page.getLong("item"))
                            .ts(jsonObject.getLong("ts"))
                            .build());
                }

                //获取曝光信息
                JSONArray displays = jsonObject.getJSONArray("displays");

                if (displays != null && displays.size() > 0) {
                    //如果是曝光数据

                    //获取单条曝光数据
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject display = displays.getJSONObject(i);

                        if ("sku_id".equals(display.getString("item_type"))) {
                            out.collect(ProductStats
                                    .builder()
                                    .sku_id(display.getLong("item"))
                                    .display_ct(1L)
                                    .ts(jsonObject.getLong("ts"))
                                    .build());
                        }

                    }
                }

            }
        });

        //3.2 商品的收藏
        SingleOutputStreamOperator<ProductStats> productStatsWithFavor = favorInfoDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);

                return ProductStats
                        .builder()
                        .favor_ct(1L)
                        .sku_id(jsonObject.getLong("sku_id"))
                        .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                        .build();
            }
        });

        //3.3 商品的加购
        SingleOutputStreamOperator<ProductStats> productStatsWithCart = cartInfoDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);

                return ProductStats
                        .builder()
                        .cart_ct(1L)
                        .sku_id(jsonObject.getLong("sku_id"))
                        .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                        .build();
            }
        });

        //3.4 商品的下单
        SingleOutputStreamOperator<ProductStats> productStatsWithOrder = orderWideDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {

                OrderWide orderWide = JSON.parseObject(value, OrderWide.class);

                //订单明细中，每一个skuId可能对应着不同的orderId
                //同一种商品，可能同时参与几个不同的活动或者满减，就可以划分到多个订单中，
                //所以，在统计商品的被下单次数、被支付次数、被退款次数时，要考虑到对orderId进行去重

                //创建set集合存放orderId
                HashSet<Long> orderIds = new HashSet<>();

                orderIds.add(orderWide.getOrder_id());

                return ProductStats
                        .builder()
                        .sku_id(orderWide.getSku_id())
                        .order_sku_num(orderWide.getSku_num())
                        .order_amount(orderWide.getSplit_total_amount())
                        .orderIdSet(orderIds)
                        .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                        .build();
            }
        });

        //3.5 商品的支付
        SingleOutputStreamOperator<ProductStats> productWithPayment = paymentWideDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                PaymentWide paymentWide = JSON.parseObject(value, PaymentWide.class);

                //创建Set集合存放skuId
                HashSet<Long> orderIds = new HashSet<>();

                orderIds.add(paymentWide.getOrder_id());

                return ProductStats.builder()
                        .sku_id(paymentWide.getSku_id())
                        .payment_amount(paymentWide.getSplit_total_amount())
                        .paidOrderIdSet(orderIds)
                        .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                        .build();
            }
        });

        //3.6 商品的退单
        SingleOutputStreamOperator<ProductStats> productWithRefund = refundInfoDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);

                //创建Set集合存放skuId
                HashSet<Long> orderIds = new HashSet<>();

                orderIds.add(jsonObject.getLong("order_id"));

                return ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .refundOrderIdSet(orderIds)
                        .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                        .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                        .build();
            }
        });

        //3.7 商品的评论
        SingleOutputStreamOperator<ProductStats> productWithCommon = commentInfoDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);

                Long goodCt = 0L;
                if (GmallConstant.APPRAISE_GOOD.equals(jsonObject.getString("appraise"))) {
                    goodCt = 1L;
                }

                return ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .comment_ct(1L)
                        .good_comment_ct(goodCt)
                        .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                        .build();
            }
        });


        //TODO 4.union各个流
        DataStream<ProductStats> union = productStatsWithClickAndDisplay.union(
                productStatsWithFavor,
                productStatsWithCart,
                productStatsWithOrder,
                productWithPayment,
                productWithRefund,
                productWithCommon
        );

        //TODO 5.生成WaterMark
        SingleOutputStreamOperator<ProductStats> unionWithWmDS = union.assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                    @Override
                    public long extractTimestamp(ProductStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        //TODO 6.分组、开窗、聚合
        SingleOutputStreamOperator<ProductStats> reduceDS = unionWithWmDS.keyBy(ProductStats::getSku_id)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats value1, ProductStats value2) throws Exception {
                        value1.setDisplay_ct(value1.getDisplay_ct() + value2.getDisplay_ct());
                        value1.setClick_ct(value1.getClick_ct() + value2.getClick_ct());
                        value1.setFavor_ct(value1.getFavor_ct() + value2.getFavor_ct());
                        value1.setCart_ct(value1.getCart_ct() + value2.getCart_ct());
                        value1.setOrder_sku_num(value1.getOrder_sku_num() + value2.getOrder_sku_num());
                        value1.setOrder_amount(value1.getOrder_amount().add(value2.getOrder_amount()));
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        //value1.setOrder_ct(value1.getOrderIdSet().size() + 0L);
                        value1.setPayment_amount(value1.getPayment_amount().add(value2.getPayment_amount()));
                        value1.getPaidOrderIdSet().addAll(value2.getPaidOrderIdSet());
                        //value1.setPaid_order_ct(value1.getPaidOrderIdSet().size() + 0L);
                        value1.setRefund_amount(value1.getRefund_amount().add(value2.getRefund_amount()));
                        value1.getRefundOrderIdSet().addAll(value2.getRefundOrderIdSet());
                        //value1.setRefund_order_ct(value1.getRefundOrderIdSet().size() +0L);
                        value1.setComment_ct(value1.getComment_ct() + value2.getComment_ct());
                        value1.setGood_comment_ct(value1.getGood_comment_ct() + value2.getGood_comment_ct());

                        return value1;
                    }
                }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void apply(Long key, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {

                        //获取窗口最后的数据
                        ProductStats productStats = input.iterator().next();

                        //补充窗口信息
                        productStats.setStt(DateTimeUtil.toYMDhms(new Date(window.getStart())));
                        productStats.setEdt(DateTimeUtil.toYMDhms(new Date(window.getEnd())));

                        //补充商品的下单、支付、退单次数
                        productStats.setOrder_ct((long) productStats.getOrderIdSet().size());
                        productStats.setPaid_order_ct((long) productStats.getPaidOrderIdSet().size());
                        productStats.setRefund_order_ct((long) productStats.getRefundOrderIdSet().size());

                        //输出数据
                        out.collect(productStats);
                    }
                });

        //TODO 7.补充维度信息
        //7.1 sku维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSku = AsyncDataStream.unorderedWait(reduceDS,
                new DimAsyncFunctionTest01<ProductStats>("DIM_SKU_INFO") {

            //查询的主键
            @Override
            public String getKey(ProductStats productStats) {
                return productStats.getSku_id().toString();
            }

            //关联维度信息
            @Override
            public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {

                productStats.setSku_name(dimInfo.getString("SKU_NAME"));
                productStats.setSku_price(dimInfo.getBigDecimal("PRICE"));
                productStats.setSpu_id(dimInfo.getLong("SPU_ID"));
                productStats.setTm_id(dimInfo.getLong("TM_ID"));
                productStats.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));

            }
        }, 60, TimeUnit.SECONDS);

        //7.2 spu维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpu = AsyncDataStream.unorderedWait(productStatsWithSku,
                new DimAsyncFunctionTest01<ProductStats>("DIM_SPU_INFO") {
            @Override
            public String getKey(ProductStats productStats) {
                return productStats.getSpu_id().toString();
            }

            @Override
            public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {

                productStats.setSpu_name(dimInfo.getString("SPU_NAME"));

            }
        }, 60, TimeUnit.SECONDS);

        //7.3 category3维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3 = AsyncDataStream.unorderedWait(productStatsWithSpu,
                new DimAsyncFunctionTest01<ProductStats>("DIM_BASE_CATEGORY3") {
            @Override
            public String getKey(ProductStats productStats) {
                return productStats.getCategory3_id().toString();
            }

            @Override
            public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {

                productStats.setSpu_name(dimInfo.getString("NAME"));

            }
        }, 60, TimeUnit.SECONDS);

        //7.4 TM
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDstream =
                AsyncDataStream.unorderedWait(productStatsWithCategory3,
                        new DimAsyncFunctionTest01<ProductStats>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setTm_name(jsonObject.getString("TM_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //TODO 8.将数据写出到ClickHouse
        productStatsWithTmDstream.print(">>>>>>>>>>");
        productStatsWithTmDstream.addSink(ClickHouseUtilTest01.getSinkFunc("insert into product_stats_210625 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 9.启动任务
        env.execute("ProductStatsApp");
    }
}
