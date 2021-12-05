package realtime02.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class MykafkaUtilTest02 {
    private static Properties properties = new Properties();

    static {
        properties.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092");
    }


    //获取kafkaSink
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), properties);
    }

    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),properties);
    }
}
