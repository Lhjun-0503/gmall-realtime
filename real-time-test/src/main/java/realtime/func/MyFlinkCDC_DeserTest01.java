package realtime.func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class MyFlinkCDC_DeserTest01 implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {
        //创建JSON对象封装最终数据
        JSONObject result = new JSONObject();

        String topic = sourceRecord.topic();
        String[] split = topic.split("\\.");

        //获取库名
        String database = split[1];

        //获取表名
        String tableName = split[2];

        Struct value = (Struct) sourceRecord.value();

        //创建JSON对象封装after数据
        JSONObject afterJson = new JSONObject();

        //获取after数据
        Struct after = value.getStruct("after");

        if (after != null) {
            Schema schema = after.schema();
            //遍历after数据
            for (Field field : schema.fields()) {
                afterJson.put(field.name(),after.get(field.name()));
            }
        }

        //创建JSON对象封装before数据
        JSONObject beforeJson = new JSONObject();

        //获取before数据
        Struct before = value.getStruct("before");

        if (before != null) {
            Schema schema = before.schema();
            for (Field field : schema.fields()) {
                //遍历before数据
                beforeJson.put(field.name(),before.get(field.name()));
            }
        }

        //获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();

        if ("create".equals(type)){
            type = "insert";
        }

        result.put("database",database);
        result.put("tableName",tableName);
        result.put("before",beforeJson);
        result.put("after",afterJson);
        result.put("type",type);

        collector.collect(result.toJSONString());


    }


    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
