package com.atguigu.gmall.realtime.func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;


public class MyFlinkCDC_Deser implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        //创建JSONObject保存最终封装的数据
        JSONObject result = new JSONObject();

        //获取sourceRecord的topic信息
        String topic = sourceRecord.topic();

        String[] split = topic.split("\\.");

        //TODO 获取数据库名
        String db = split[1];

        //TODO 获取表名
        String tableName = split[2];

        //获取value数据   转为Struct
        Struct value = (Struct) sourceRecord.value();


        //创建JSONObject保存after信息
        JSONObject afterJSON = new JSONObject();

        //TODO 获取更改后的数据  after
        Struct after = value.getStruct("after");

        //判断after是否存在
        if (after != null) {
            Schema schema = after.schema();

            //遍历after，把数据封装到afterJSON
            for (Field field : schema.fields()) {
                afterJSON.put(field.name(),after.get(field.name()));
            }
        }

        //创建JSONObject保存before信息
        JSONObject beforeJSON = new JSONObject();

        //TODO 获取更改前的数据  before
        Struct before = value.getStruct("before");

        //判断before是否存在
        if (before != null) {
            Schema schema = before.schema();

            //遍历before，把数据封装到beforeJSON
            for (Field field : schema.fields()) {
                beforeJSON.put(field.name(),before.get(field.name()));
            }
        }

        //TODO 获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);

        String type = operation.toString().toLowerCase();

        if ("create".equals(type)) {
            type = "insert";
        }

        //TODO 封装最终的数据
        result.put("database",db);
        result.put("tableName",tableName);
        result.put("type",type);
        result.put("before",beforeJSON);
        result.put("after",afterJSON);



        collector.collect(result.toJSONString());


    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
