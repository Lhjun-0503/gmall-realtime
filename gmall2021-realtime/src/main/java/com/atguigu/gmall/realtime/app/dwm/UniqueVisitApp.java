package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
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
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

//数据流向   Web/App  ->  Nginx  ->  日志服务器  ->  Kafka(ods)  ->  FlinkApp  ->  Kafka(dwd)    ->  FlinkApp  ->  kafka(dwm)
//程序       jar包    ->  Nginx  ->  logger.sh  ->  kafka(zk)   ->  BaseLogApp  ->  kafka(zk)   ->  UniqueVisitApp  ->  kafka(zk)

public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //1.1设置状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));

        //1.2开启CK
        //env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000L);

        //2.读取kafka的dwd_page_log主题数据创建流
        String groupId = "unique_visit_app";

        String sourceTopic = "dwd_page_log";

        String sinkTopic = "dwm_unique_visit";

        //获取kafkaSource
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);

        //从头开始消费
        //kafkaSource.setStartFromEarliest();

        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //3.将每行数据转为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

                try {
                    JSONObject jsonObject = JSON.parseObject(value);

                    out.collect(jsonObject);
                } catch (Exception e) {

                    //防止有脏数据
                    ctx.output(new OutputTag<String>("dirty") {
                    }, value);
                }


            }
        });

        //4.按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

        //5.过滤掉不是今天第一次访问的数据
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            //声明状态
            private ValueState<String> firstVisitState;
            private SimpleDateFormat simpleDateFormat;


            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态

                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

                ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("visit-state", String.class);

                //创建状态TTL配置项

                //给状态设置24小时过期，并且在状态被创建或者被修改时，过期的倒计时要被重置为24小时

                //因为状态值主要用于筛选是否今天来过，
                // 所以这个记录过了今天基本上没有用了，
                // 这里enableTimeToLive 设定了1天的过期时间，避免状态过大。

                StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                stringValueStateDescriptor.enableTimeToLive(stateTtlConfig);


                firstVisitState = getRuntimeContext().getState(stringValueStateDescriptor);

            }

            @Override
            public boolean filter(JSONObject value) throws Exception {

                //取出上一次访问页面
                String lastPageId = value.getJSONObject("page").getString("last_page_id");

                //判断是否存在上一个页面
                if (lastPageId == null || lastPageId.length() <= 0) {

                    //不存在上一个页面  为用户访问的首页

                    //取出状态
                    String firstVisitDate = firstVisitState.value();

                    //取出数据时间   （首页的访问时间）
                    Long ts = value.getLong("ts");
                    String curDate = simpleDateFormat.format(ts);

                    if (firstVisitDate == null || !firstVisitDate.equals(curDate)) {
                        //状态中没有首次访问日期  或者  状态中的首次访问时间和当前的访问时间（首页访问时间）不同

                        //说明当前数据是当日用户首次登录的
                        //保留该数据

                        //更新状态
                        firstVisitState.update(curDate);

                        return true;
                    } else {
                        //状态中保存了首次访问时间   或者  状态中的首次访问时间和当前的访问时间（首页访问时间）相同

                        //说明当前数据不是当日用户首次登录的

                        //过滤该数据
                        return false;
                    }
                } else {

                    //存在last_page_id  则该条数据不是用户访问首页的数据

                    //过滤该数据
                    return false;
                }


            }
        });

        //TODO 6.将过滤后的数据写入kafka的dwm_unique_visit
        filterDS.map(new MapFunction<JSONObject, String>() {
            @Override
            public String map(JSONObject value) throws Exception {
                return value.toString();
            }
        })
                .addSink(MyKafkaUtil.getKafkaSink(sinkTopic));



        //打印测试
        filterDS.print(">>>>>>>>>");




        //执行任务
        env.execute();
    }
}
