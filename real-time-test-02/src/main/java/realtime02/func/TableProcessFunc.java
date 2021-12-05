package realtime02.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ibm.icu.util.Output;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.ArrayUtils;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.StringUtils;
import realtime02.bean.TableProcess;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class TableProcessFunc extends BroadcastProcessFunction<JSONObject, String, String> {

    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private OutputTag<String> hbaseOutput;

    public TableProcessFunc(MapStateDescriptor<String, TableProcess> mapStateDescriptor, OutputTag<String> hbaseOutput) {
        this.mapStateDescriptor = mapStateDescriptor;
        this.hbaseOutput = hbaseOutput;
    }

    //处理广播过来的数据
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {

        JSONObject after = JSON.parseObject(value).getJSONObject("after");

        //广播过来的数据转为JavBean
        TableProcess tableProcess = JSON.parseObject(after.toJSONString(), TableProcess.class);


        //操作类型
        String type = tableProcess.getOperateType();

        //表名
        String tableName = tableProcess.getSourceTable();

        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        //把封装好的数据广播
        broadcastState.put(type + ":" + tableName, tableProcess);

    }

    //处理主流的数据
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<String> out) throws Exception {

        JSONObject after = value.getJSONObject("after");

        String type = value.getString("type");

        String tableName = value.getString("tableName");

        String key = type + ":" + tableName;

        //获取广播状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {

            //过滤字段
            filterColumns(after,tableProcess.getSinkColumns());

            //主流数据中加上要输出的表
            after.put("sinkTable",tableProcess.getSinkTable());


            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                //输出到HBase
                ctx.output(hbaseOutput, after.toString());
            } else if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                out.collect(after.toString());
            }
        } else {
            System.out.println("找不到key：" + key);
        }
    }

    //过滤字段
    private void filterColumns(JSONObject after, String sinkColumns) {

        List<String> columns = Arrays.asList(sinkColumns.split(","));

        after.keySet().removeIf(next -> !columns.contains(next));
    }
}
