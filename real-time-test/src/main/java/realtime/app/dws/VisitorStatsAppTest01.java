package realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import realtime.bean.VisitorStats;
import realtime.utils.ClickHouseUtilTest01;
import realtime.utils.DateTimeUtil;
import realtime.utils.MyKafkaUtilTest01;

import java.time.Duration;
import java.util.Date;

public class VisitorStatsAppTest01 {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        //TODO 2.读取kafka数据  dwd_page_log  dwm_unique_visit  dwm_user_jump_detail
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        String groupId = "visitor-stats-app-test01";

        DataStreamSource<String> pageViewDS = env.addSource(MyKafkaUtilTest01.getKafkaSource(pageViewSourceTopic, groupId));
        DataStreamSource<String> uniqueVisitDS = env.addSource(MyKafkaUtilTest01.getKafkaSource(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> userJumpDetailDS = env.addSource(MyKafkaUtilTest01.getKafkaSource(userJumpDetailSourceTopic, groupId));

        //TODO 3.将数据转为统一的JavaBean
        //pageView
        SingleOutputStreamOperator<VisitorStats> visitorWithPvDS = pageViewDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");
                //版本
                String vc = common.getString("vc");
                //渠道
                String ch = common.getString("ch");
                //地区
                String ar = common.getString("ar");
                //新老客户标识
                String isNew = common.getString("is_new");

                Long ts = jsonObject.getLong("ts");

                JSONObject page = jsonObject.getJSONObject("page");

                //持续访问时间
                Long during_time = page.getLong("during_time");

                Long sv = 0L;

                if (page.getString("last_page_id") == null) {
                    //如果为首页，则进入次数为一次
                    sv = 1L;
                }

                return new VisitorStats("", "",
                        vc,
                        ch,
                        ar,
                        isNew,
                        0L, 1L, sv, 0L, during_time,
                        ts);
            }
        });

        //uniqueVisit
        SingleOutputStreamOperator<VisitorStats> visitorWithUvDS = uniqueVisitDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");
                //版本
                String vc = common.getString("vc");
                //渠道
                String ch = common.getString("ch");
                //地区
                String ar = common.getString("ar");
                //新老客户标识
                String isNew = common.getString("is_new");

                Long ts = jsonObject.getLong("ts");

                return new VisitorStats("", "",
                        vc,
                        ch,
                        ar,
                        isNew,
                        1L, 0L, 0L, 0L, 0L,
                        ts);
            }
        });

        //userJump
        SingleOutputStreamOperator<VisitorStats> visitorWithUjDS = userJumpDetailDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");
                //版本
                String vc = common.getString("vc");
                //渠道
                String ch = common.getString("ch");
                //地区
                String ar = common.getString("ar");
                //新老客户标识
                String isNew = common.getString("is_new");

                Long ts = jsonObject.getLong("ts");

                return new VisitorStats("", "",
                        vc,
                        ch,
                        ar,
                        isNew,
                        0L, 0L, 0L, 1L, 0L,
                        ts);
            }
        });


        //TODO 4.三个流Union
        DataStream<VisitorStats> unionDS = visitorWithPvDS.union(visitorWithUvDS, visitorWithUjDS);


        //TODO 5.提取时间戳生成WaterMark
        SingleOutputStreamOperator<VisitorStats> unionDSWithWm = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(13))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        //TODO 6.分组、开窗、聚合
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDS = unionDSWithWm.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<>(
                        value.getVc(),
                        value.getCh(),
                        value.getAr(),
                        value.getIs_new()
                );
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //reduce里面使用增量函数ReduceFunction，时效性好，状态保存的数据量少,但是使用增量函数不能获取窗口信息
        //reduce里面也能用全量函数WindowFunction，在窗口关闭之前调用一次WindowFunction，可以获取到窗口信息
        SingleOutputStreamOperator<VisitorStats> resultDS = windowDS.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());

                return value1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {

                //取出最后的数据
                VisitorStats visitorStats = input.iterator().next();

                //补全JavaBean字段
                visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(window.getStart())));
                visitorStats.setEdt(DateTimeUtil.toYMDhms(new Date(window.getEnd())));

                //输出数据
                out.collect(visitorStats);
            }
        });


        //打印测试
        resultDS.map(JSONObject::toJSONString).print("visitor>>>>>>");

        //TODO 7.数据写入ClickHouse
        resultDS.addSink(ClickHouseUtilTest01.getSinkFunc("insert into visitor_stats_210625 values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 8.执行
        env.execute("VisitorStatsAppTest01");

    }
}
