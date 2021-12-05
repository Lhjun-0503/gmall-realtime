package realtime02.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import realtime02.utils.MykafkaUtilTest02;

public class BaseLogAppTest02 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        String logSourceTopic = "ods_base_log";

        String pageSinkTopic = "dwd_page_log";

        String startSinkTopic = "dwd_start_log";

        String displaySinkTopic = "dwd_display_log";

        String groupId = "base-log-app-test02";

        DataStreamSource<String> kafkaDS = env.addSource(MykafkaUtilTest02.getKafkaSource(logSourceTopic, groupId).setStartFromEarliest());


        //数据转为JSON对象,脏数据放到侧输出流
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObj = JSONObject.parseObject(value);

                    out.collect(jsonObj);
                } catch (Exception e) {

                    ctx.output(new OutputTag<String>("脏数据"){}, value);
                }
            }
        });


        //按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonDS.keyBy(json -> json.getJSONObject("common").getString("mid"));


        //新客户校验
        SingleOutputStreamOperator<JSONObject> logCheckDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            //声明状态，记录用户有登录过
            public ValueState<String> firstState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态
                firstState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {

                JSONObject common = value.getJSONObject("common");

                String is_new = common.getString("is_new");

                if ("1".equals(is_new)) {

                    //取出状态
                    String firstStateValue = firstState.value();

                    if (firstStateValue == null) {

                        //更新状态
                        firstState.update("1");

                    } else {

                        value.getJSONObject("common").put("is_new", "0");
                    }
                }

                return value;
            }
        });

        //按照日志类型分流
        //启动、页面（页面里面可能有曝光日志）
        OutputTag<JSONObject> pageOutput = new OutputTag<JSONObject>("page") {
        };

        OutputTag<JSONObject> displaysOutput = new OutputTag<JSONObject>("displays") {
        };
        SingleOutputStreamOperator<JSONObject> startDS = logCheckDS.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {

                if (value.getJSONObject("start") != null) {
                    //启动日志
                    out.collect(value);

                } else if (value.getJSONObject("page") != null) {
                    //页面日志
                    ctx.output(pageOutput, value);

                    //判断是否有曝光日志
                    if (value.getJSONArray("displays") != null) {

                        JSONArray displays = value.getJSONArray("displays");

                        for (int i = 0; i < displays.size(); i++) {

                            JSONObject display = displays.getJSONObject(i);

                            display.put("page_id",value.getJSONObject("page").getString("page_id"));

                            ctx.output(displaysOutput, display);
                        }


                    }
                }

            }
        });

        DataStream<JSONObject> pageDS = startDS.getSideOutput(pageOutput);

        DataStream<JSONObject> displaysDS = startDS.getSideOutput(displaysOutput);

        startDS.print("start>>>>>>>");

        pageDS.print("page>>>>>>>>>");

        displaysDS.print("displays>>>>>>>");


        //写入kafka
        startDS.map((MapFunction<JSONObject, String>) JSONAware::toJSONString).addSink(MykafkaUtilTest02.getKafkaSink(startSinkTopic));

        pageDS.map((MapFunction<JSONObject, String>) JSONAware::toJSONString).addSink(MykafkaUtilTest02.getKafkaSink(pageSinkTopic));

        displaysDS.map((MapFunction<JSONObject, String>) JSONAware::toJSONString).addSink(MykafkaUtilTest02.getKafkaSink(displaySinkTopic));


        env.execute();
    }
}
