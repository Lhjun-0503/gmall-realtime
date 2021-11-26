package com.atguigu.gmall.realtime.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    //定义侧输出流标签
    private OutputTag<JSONObject> outputTag;

    //定义Map状态表述器
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    //定义Phoenix的连接
    private Connection connection = null;

    public TableProcessFunction(OutputTag<JSONObject> outputTag,MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化Phoenix的连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }



    //处理广播过来的数据
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

        //广播流数据  {"database":"gmall_realtime_process","before":{},"after":{""},"type":"insert","tableName":"table_process"}


        //1.将数据转为JavaBean
        JSONObject jsonObject = JSON.parseObject(value);
        JSONObject data = jsonObject.getJSONObject("after");
        TableProcess tableProcess = JSON.parseObject(data.toJSONString(), TableProcess.class);

        if (tableProcess != null) {

            //2.校验表是否存在，如果不存在，

            // 再判断sink_type类型，在phoenix建表
            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                checkTable(tableProcess.getSinkTable(),
                        tableProcess.getSinkColumns(),
                        tableProcess.getSinkPk(),
                        tableProcess.getSinkExtend());
            }

            //3.将数据写入状态广播处理
            BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
            String key = tableProcess.getSourceTable() + ":" + tableProcess.getOperateType();
            broadcastState.put(key,tableProcess);
        }


    }

    /**
     * 核心处理方法，根据MySQL配置表的信息为每条数据打标签，走Kafka还是HBase
     *
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //主流的数据{"database":"gmall_realtime","before":{},"after":{""},"type":"insert","tableName":"user_info"}

        //获取状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        //System.out.println("主流："+value.toString());

        //在主流中获取表名和操作类型
        String table = value.getString("tableName");

        String type = value.getString("type");

        String key = table + ":" + type;
        //System.out.println("主流的key： "+key);

        //取出对应的配置信息
        TableProcess tableProcess = broadcastState.get(key);

        //System.out.println("从状态中得到tb： "+tableProcess);

        if (tableProcess != null) {

            //向数据汇中追加sink_table信息
            value.put("sink_table", tableProcess.getSinkTable());

            //根据配置信息中提供的字段做数据过滤
            filterColumn(value.getJSONObject("after"), tableProcess.getSinkColumns());

            //判断当前数据应该写往HBase还是Kafka
            if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                //kafka数据，将数据输出到主流
                out.collect(value);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
                //HBase数据，将数据输出到侧输出流
                ctx.output(outputTag,value);
            }
        } else {
            System.out.println("No Key " + key + " In Mysql");
        }
    }

    /**
     * Phoenix建表
     *
     * @param sinkTable   表名
     * @param sinkColumns 表名字段  test
     * @param sinkPk      表主键   id,name,sex
     * @param sinkExtend  表扩展字段
     *                    create table if not exists mydb.test(id varchar primary key,name varchar,sex varchar) ...
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        //给主键以及扩展字段赋默认值
        if (sinkPk == null) {
            sinkPk = "id";
        }

        if (sinkExtend == null) {
            sinkExtend = "";
        }

        //封装建表SQL
        StringBuilder createSql = new StringBuilder("create table if not exists ").append(GmallConfig.HBASE_SCHEMA).append(".").append(sinkTable).append("(");

        //遍历添加字段信息
        String[] fields = sinkColumns.split(",");

        for (int i = 0; i < fields.length; i++) {

            //取出字段
            String field = fields[i];

            //判断当前字段是否为主键
            if (sinkPk.equals(field)) {
                createSql.append(field).append(" varchar primary key");
            } else {
                createSql.append(field).append(" varchar");
            }

            //如果当前字段不是最后一个字段，则追加","
            if (i < fields.length - 1) {
                createSql.append(",");
            }
        }

        createSql.append(")");
        createSql.append(sinkExtend);

        System.out.println(createSql);

        //执行建表SQL
        PreparedStatement preparedStatement = null;

        try {
            preparedStatement = connection.prepareStatement(createSql.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("创建Phoenix表" + sinkTable + "失败！");
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
     * 校验字段，过滤掉多余的字段
     *
     * @param data        待过滤的数据
     * @param sinkColumns 目标字段
     */
    private void filterColumn(JSONObject data, String sinkColumns) {

        //保留的数据字段
        String[] fields = sinkColumns.split(",");

        List<String> fieldList = Arrays.asList(fields);

        Set<Map.Entry<String, Object>> entries = data.entrySet();

        //while (iterator.hasNext()) {
        //    Map.Entry<String, Object> next = iterator.next();
        //    if (!fieldList.contains(next.getKey())) {
        //        iterator.remove();
        //    }
        //}

        entries.removeIf(next -> !fieldList.contains(next.getKey()));
    }
}
