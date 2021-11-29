package realtime.utils;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

public class MyKafkaUtilTest01 {
    private static String KAFKA_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static Properties properties = new Properties();
    private static String DEFAULT_TOPIC = "dwd_default_topic";

    static {
        properties.setProperty("bootstrap.servers", KAFKA_SERVER);
    }


    /**
     * 获取KafkaSink的方法  给定确定的topic
     * @param topic
     * @return
     */
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), properties);
    }


    public static <T>FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema) {

        return new FlinkKafkaProducer<T>(
                DEFAULT_TOPIC,
                kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );

    }

    /**
     * 获取KafkaSource的方法
     * @param topic   主题
     * @param groupId  消费者组
     * @return
     */
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic,String groupId){
        //给配置信息对象添加配置项
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);

        //获取KafkaSource
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),properties);
    }





}
