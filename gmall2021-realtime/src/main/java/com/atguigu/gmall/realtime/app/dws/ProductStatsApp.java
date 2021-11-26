package com.atguigu.gmall.realtime.app.dws;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.PaymentWide;
import com.atguigu.gmall.realtime.bean.ProductStats;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.utils.ClickhouseUtil;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * Desc: 形成以商品为准的统计  曝光 点击  购物车  下单 支付  退单  评论数 宽表
 */
public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 2.读取kafka数据
        //dwm_order_wide  dwm_payment_wide
        //dwd_page_log、dwd_cart_info、dwd_favor_info、dwd_order_refund_info、dwd_comment_info
        String groupId = "product-stats-app";

        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String pageViewSourceTopic = "dwd_page_log";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commonInfoSourceTopic = "dwd_comment_info";

        DataStreamSource<String> orderWideDS = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId)/*.setStartFromEarliest()*/);
        DataStreamSource<String> payWideDS = env.addSource(MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId)/*.setStartFromEarliest()*/);
        DataStreamSource<String> pageViewDS = env.addSource(MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId)/*.setStartFromEarliest()*/);
        DataStreamSource<String> cartDS = env.addSource(MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId)/*.setStartFromEarliest()*/);
        DataStreamSource<String> favorDS = env.addSource(MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId)/*.setStartFromEarliest()*/);
        DataStreamSource<String> refundDS = env.addSource(MyKafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId)/*.setStartFromEarliest()*/);
        DataStreamSource<String> commonDS = env.addSource(MyKafkaUtil.getKafkaSource(commonInfoSourceTopic, groupId)/*.setStartFromEarliest()*/);

        //TODO 3.将数据转为统一的JavaBean
        //3.1点击和曝光
        SingleOutputStreamOperator<ProductStats> productStatsWithPageDS = pageViewDS.process(new ProcessFunction<String, ProductStats>() {
            @Override
            public void processElement(String value, Context ctx, Collector<ProductStats> out) throws Exception {

                //将数据转为JSON对象
                JSONObject jsonObject = JSON.parseObject(value);

                //获取数据时间
                Long ts = jsonObject.getLong("ts");

                //获取页面信息
                JSONObject page = jsonObject.getJSONObject("page");
                String pageId = page.getString("page_id");

                String itemType = page.getString("item_type");

                if ("good_detail".equals(pageId) && "sku_id".equals(itemType)) {

                    //取出被点击的商品ID
                    Long item = page.getLong("item");

                    out.collect(ProductStats.builder()
                            .sku_id(item)
                            .click_ct(1L)
                            .ts(ts)
                            .build());
                }

                //取出曝光数据
                JSONArray displays = jsonObject.getJSONArray("displays");

                //判断是否为曝光数据
                if (displays != null && displays.size() > 0) {

                    //遍历曝光数据
                    for (int i = 0; i < displays.size(); i++) {

                        JSONObject displaysJsonObj = displays.getJSONObject(i);

                        //取出当前曝光的数据类型
                        String item_type = displaysJsonObj.getString("item_type");

                        //商品曝光数据
                        if ("sku_id".equals(item_type)) {
                            out.collect(ProductStats.builder()
                                    .sku_id(displaysJsonObj.getLong("item"))
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build());
                        }
                    }
                }
            }
        });

        //3.2收藏
        SingleOutputStreamOperator<ProductStats> productStatsWithFavorDS = favorDS.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                //将数据转为JSON对象
                JSONObject jsonObject = JSON.parseObject(value);

                //封装对象并返回
                return ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .favor_ct(1L)
                        .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                        .build();
            }
        });

        //3.3加购
        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartDS.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                //将数据转为JSON对象
                JSONObject jsonObject = JSON.parseObject(value);

                //封装数据并返回
                return ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .cart_ct(jsonObject.getLong("sku_num"))
                        .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                        .build();
            }
        });

        //3.4订单
        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderWideDS.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {

                //将数据转为OrderWide
                OrderWide orderWide = JSON.parseObject(value, OrderWide.class);

                //创建集合用于存放订单ID，考虑去重
                HashSet<Object> hashSet = new HashSet<>();
                hashSet.add(orderWide.getOrder_id());

                //封装对象并返回
                return ProductStats.builder()
                        .sku_id(orderWide.getSku_id())
                        .order_sku_num(orderWide.getSku_num())
                        .order_amount(orderWide.getTotal_amount())
                        .orderIdSet(hashSet)
                        .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                        .build();
            }
        });

        //3.5支付
        SingleOutputStreamOperator<ProductStats> productStatsWithPayDS = payWideDS.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                //将数据转为PaymentWide
                PaymentWide paymentWide = JSON.parseObject(value, PaymentWide.class);

                //创建集合用于存放订单ID
                HashSet<Object> hashSet = new HashSet<>();
                hashSet.add(paymentWide.getOrder_id());

                //封装对象并返回
                return ProductStats.builder()
                        .sku_id(paymentWide.getSku_id())
                        .payment_amount(paymentWide.getTotal_amount())
                        .paidOrderIdSet(hashSet)
                        .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                        .build();
            }
        });

        //3.6退单
        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundDS.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                //将数据转为JSON对象
                JSONObject jsonObject = JSON.parseObject(value);

                //创建集合用于存放订单ID
                HashSet<Object> hashSet = new HashSet<>();
                hashSet.add(jsonObject.getLong("order_id"));

                //封装对象并返回
                return ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                        .refundOrderIdSet(hashSet)
                        .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                        .build();
            }
        });

        //3.7评价
        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commonDS.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                //将数据转为JSON对象
                JSONObject jsonObject = JSON.parseObject(value);

                //获取评价的类型数据
                String appraise = jsonObject.getString("appraise");
                Long goodCt = 0L;
                if (GmallConstant.APPRAISE_GOOD.equals(appraise)) {
                    goodCt = 1L;
                }

                //封装对象并返回
                return ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .comment_ct(1L)
                        .good_comment_ct(goodCt)
                        .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                        .build();
            }
        });

        //TODO 4.将各个流union
        DataStream<ProductStats> unionDS = productStatsWithPageDS.union(productStatsWithFavorDS,
                productStatsWithCartDS,
                productStatsWithOrderDS,
                productStatsWithPayDS,
                productStatsWithRefundDS,
                productStatsWithCommentDS);

        //TODO 5.提取时间戳生成WaterMark
        SingleOutputStreamOperator<ProductStats> productStatsWithWmDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                    @Override
                    public long extractTimestamp(ProductStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        //TODO 6.分组、开窗、聚合
        KeyedStream<ProductStats, Long> keyedStream = productStatsWithWmDS.keyBy(ProductStats::getSku_id);

        SingleOutputStreamOperator<ProductStats> reduceDS = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats value1, ProductStats value2) throws Exception {
                        value1.setDisplay_ct(value1.getDisplay_ct() + value2.getDisplay_ct());
                        value1.setClick_ct(value1.getClick_ct() + value2.getClick_ct());

                        value1.setCart_ct(value1.getCart_ct() + value2.getCart_ct());

                        value1.setFavor_ct(value1.getFavor_ct() + value2.getFavor_ct());

                        value1.setOrder_amount(value1.getOrder_amount().add(value2.getOrder_amount()));
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        value1.setOrder_ct(value1.getOrderIdSet().size() + 0L);
                        value1.setOrder_sku_num(value1.getOrder_sku_num() + value2.getOrder_sku_num());

                        value1.getRefundOrderIdSet().addAll(value2.getRefundOrderIdSet());
                        value1.setRefund_order_ct(value1.getRefundOrderIdSet().size() + 0L);
                        value1.setRefund_amount(value1.getRefund_amount().add(value2.getRefund_amount()));

                        value1.getPaidOrderIdSet().addAll(value2.getPaidOrderIdSet());
                        value1.setPaid_order_ct(value1.getPaidOrderIdSet().size() + 0L);
                        value1.setPayment_amount(value1.getPayment_amount().add(value2.getPayment_amount()));

                        value1.setComment_ct(value1.getComment_ct() + value2.getComment_ct());
                        value1.setGood_comment_ct(value1.getGood_comment_ct() + value2.getGood_comment_ct());

                        return value1;
                    }
                }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {

                        //取出数据
                        ProductStats productStats = input.iterator().next();

                        //处理开窗开始和结束时间
                        productStats.setStt(DateTimeUtil.toYMDhms(new Date(window.getStart())));
                        productStats.setEdt(DateTimeUtil.toYMDhms(new Date(window.getEnd())));

                        //处理订单数量
                        productStats.setOrder_ct(productStats.getOrderIdSet().size() + 0L);
                        productStats.setPaid_order_ct(productStats.getPaidOrderIdSet().size() + 0L);
                        productStats.setRefund_order_ct(productStats.getRefundOrderIdSet().size() + 0L);

                        //写出数据
                        out.collect(productStats);
                    }
                });



        //TODO 7.关联维度数据
        //7.1关联SKU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(reduceDS,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {

                    //获取要关联的主键
                    @Override
                    public String getId(ProductStats input) {
                        return input.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats input, JSONObject dimInfo) throws Exception {

                        //取出维度信息
                        BigDecimal price = dimInfo.getBigDecimal("PRICE");
                        String sku_name = dimInfo.getString("SKU_NAME");
                        Long spu_id = dimInfo.getLong("SPU_ID");
                        Long tm_id = dimInfo.getLong("TM_ID");
                        Long category3_id = dimInfo.getLong("CATEGORY3_ID");

                        //将信息写入productStats对象
                        input.setSku_price(price);
                        input.setSku_name(sku_name);
                        input.setSpu_id(spu_id);
                        input.setTm_id(tm_id);
                        input.setCategory3_id(category3_id);
                    }
                }, 60,
                TimeUnit.SECONDS);

        //7.2关联SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS = AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {

                    //获取要关联的主键
                    @Override
                    public String getId(ProductStats input) {
                        return input.getSpu_id().toString();
                    }

                    @Override
                    public void join(ProductStats input, JSONObject dimInfo) throws Exception {

                        //取出维度信息
                        String spu_name = dimInfo.getString("SPU_NAME");

                        //将信息写入productStats对象
                        input.setSpu_name(spu_name);

                    }
                }, 60,
                TimeUnit.SECONDS);

        //7.3关联Trademark维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS = AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {

                    //获取要关联的主键
                    @Override
                    public String getId(ProductStats input) {
                        return input.getTm_id().toString();
                    }

                    @Override
                    public void join(ProductStats input, JSONObject dimInfo) throws Exception {

                        //取出维度信息
                        String tm_name = dimInfo.getString("TM_NAME");

                        //将信息写入productStats对象
                        input.setTm_name(tm_name);

                    }
                }, 60,
                TimeUnit.SECONDS);

        //7.4关联Category维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS = AsyncDataStream.unorderedWait(productStatsWithTmDS,
                new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {

                    //获取要关联的主键
                    @Override
                    public String getId(ProductStats input) {
                        return input.getCategory3_id().toString();
                    }

                    @Override
                    public void join(ProductStats input, JSONObject dimInfo) throws Exception {

                        //取出维度信息
                        String name = dimInfo.getString("NAME");

                        //将信息写入productStats对象
                        input.setCategory3_name(name);
                    }
                }, 60,
                TimeUnit.SECONDS);



        //打印测试
        productStatsWithCategory3DS.map(new MapFunction<ProductStats, String>() {
            @Override
            public String map(ProductStats value) throws Exception {
                return JSONObject.toJSONString(value);
            }
        }).print();

        //TODO 8.写入ClickHouse
        productStatsWithCategory3DS.addSink(ClickhouseUtil.getSinkFunc("insert into product_stats_210625 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));


        //TODO 9.执行任务
        env.execute();
    }
}
