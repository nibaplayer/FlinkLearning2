package Kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.ArrayList;
import java.util.Properties;

public class FlinkKafkaProducer1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ArrayList<String> wordlist =new ArrayList<>();
        wordlist.add("hello");
        wordlist.add("world");

        DataStreamSource<String> dataStreamSource = env.fromCollection(wordlist);

        //创建kafka配置信息
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        //创建Flink下的kafka生产者
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "first",
                new SimpleStringSchema(),//序列化方式
                properties
        );
        dataStreamSource.addSink(kafkaProducer);//flink流输出到kafka生产者

        env.execute();
    }
}
