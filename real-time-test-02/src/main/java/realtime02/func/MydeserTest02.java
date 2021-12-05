package realtime02.func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class MydeserTest02 implements DebeziumDeserializationSchema<String> {

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

        //创建封装最终数据的JSON
        JSONObject result = new JSONObject();

        //创建封装修改前的数据的JSON
        JSONObject beforeJson = new JSONObject();

        //创建封装修改后的数据的JSON
        JSONObject afterJson = new JSONObject();

        String topic = sourceRecord.topic();

        String[] split = topic.split("\\.");

        //库名
        String db = split[1];

        //表名
        String tableName = split[2];

        Struct value = (Struct) sourceRecord.value();


        Struct after = value.getStruct("after");

        if (after != null) {

            Schema schema = after.schema();

            for (Field field : schema.fields()) {

                afterJson.put(field.name(),after.get(field.name()));

            }
        }

        Struct before = value.getStruct("before");

        if (before != null) {

            Schema schema = before.schema();

            for (Field field : schema.fields()) {

                beforeJson.put(field.name(),before.get(field.name()));
            }
        }


        //操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);

        String type = operation.toString().toLowerCase();

        if ("create".equals(type)) {

            type = "insert";
        }

        result.put("database",db);
        result.put("tableName",tableName);
        result.put("before",beforeJson);
        result.put("after",afterJson);
        result.put("type",type);

        //最终数据转为JSON字符串输出
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
