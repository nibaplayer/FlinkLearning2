package Kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class FlinkKafkaConsumer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //配置kafka信息
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");

        org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer<String> kafkaConsumer = new org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer<>(
                "first",
                new SimpleStringSchema(),
                properties
        );
        env
                .addSource(kafkaConsumer)
                .print();
        env.execute();
    }
}
