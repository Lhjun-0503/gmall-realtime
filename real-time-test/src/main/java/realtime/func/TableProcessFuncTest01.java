package realtime.func;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import realtime.bean.TableProcess;
import realtime.common.GmallConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFuncTest01 extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    //Phoenix连接
    private Connection connection;

    //广播状态
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    //侧输出流标签
    private OutputTag<JSONObject> hbaseOutput;

    public TableProcessFuncTest01(MapStateDescriptor<String, TableProcess> mapStateDescriptor,OutputTag<JSONObject> hbaseOutput) {
        this.mapStateDescriptor = mapStateDescriptor;
        this.hbaseOutput = hbaseOutput;
    }

    //获取Phoenix连接
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);

        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    /**
     * 处理广播流的数据
     *
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

        JSONObject jsonObject = JSONObject.parseObject(value);

        //获取after
        String after = jsonObject.getString("after");

        //封装为JavaBean
        TableProcess tableProcess = JSON.parseObject(after, TableProcess.class);

        //获取操作类型
        String operateType = tableProcess.getOperateType();

        //获取表名
        String sourceTable = tableProcess.getSourceTable();

        //获取sink_table
        String sinkType = tableProcess.getSinkType();

        //判断是否是维度数据，再去Phoenix建表
        if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {

            //Phoenix建表
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }

        //获取状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        broadcastState.put(operateType + ":" + sourceTable, tableProcess);




    }

    //Phoenix建表
    private void checkTable(String sink_table, String sink_columns, String sink_pk, String sink_extend) {
        if (sink_pk == null) {
            sink_pk = "id";
        }

        if (sink_extend == null) {
            sink_extend = "";
        }


        //创建建表语句
        StringBuilder sql = new StringBuilder("create table if not exists ");

        //追加库名表名
        sql.append(GmallConfig.HBASE_SCHEMA).append(".").append(sink_table).append(" (");

        //追加字段
        String[] columns = sink_columns.split(",");
        for (int i = 0; i < columns.length; i++) {

            sql.append(columns[i]).append(" varchar");

            //判断是否为主键
            if (sink_pk.equals(columns[i])) {
                sql.append(" primary key");
            }

            //判断是否为最后一个字段
            if (i < columns.length - 1) {
                sql.append(",");
            }
        }

        sql.append(")").append(sink_extend);

        //创建预编译对象
        PreparedStatement preparedStatement = null;

        try {
            preparedStatement = connection.prepareStatement(sql.toString());

            preparedStatement.execute();

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Phoenix建表" + sink_table + "失败");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }


    }

    /**
     * 处理主流数据
     *
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

        //解析主流数据
        JSONObject after = value.getJSONObject("after");


        //获取操作类型
        String type = value.getString("type");

        //获取表名
        String tableName = value.getString("tableName");

        //从广播状态查询对应的数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        TableProcess tableProcess = broadcastState.get(type + ":" + tableName);


        if (tableProcess != null) {

            //从广播状态中获取sinkTable
            String sinkTable = tableProcess.getSinkTable();

            //过滤字段
            filter(value, tableProcess.getSinkColumns());

            value.put("sinkTable",sinkTable);

            if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                out.collect(value);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                ctx.output(hbaseOutput,value);
            }
        } else {
            System.out.println("key不存在:" + type + ":" + tableName);
        }
    }

    //过滤字段
    private void filter(JSONObject value, String sinkColumns) {
        String[] columns = sinkColumns.split(",");
        List<String> strings = Arrays.asList(columns);


        JSONObject after = value.getJSONObject("after");
        Set<Map.Entry<String, Object>> entries = after.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()) {

            Map.Entry<String, Object> next = iterator.next();

            if (!strings.contains(next.getKey())) {
                iterator.remove();
            }
        }
    }
}
