package Kafka;

import Kafka.MyData;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class SingleDataProducer {
    public static void main(int windowSize) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.noRestart());

        DataGeneratorSource<MyData> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, MyData>() {
                    @Override
                    public MyData map(Long value) throws Exception {
                        return new MyData(1L,System.currentTimeMillis(),Math.random());
                    }
                },
                110000000L,
                RateLimiterStrategy.noOp(),
                Types.POJO(MyData.class)
        );

        DataStreamSource<MyData> ds = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-source");

        KafkaSink<MyData> kafkaSink = KafkaSink.<MyData>builder()
                .setBootstrapServers("hadoop102:9092")
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<MyData>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(MyData element, KafkaSinkContext context, Long timestamp) {
                                return new ProducerRecord<byte[], byte[]>("demo",MyData.getByteArray(element.timestamp),element.Serialization());
                            }
                        }
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("hao-")
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,10*60*1000+"")
                .build();

        ds
                .countWindowAll(windowSize)
                .aggregate(
                        new AggregateFunction<MyData, MyData, MyData>() {
                            @Override
                            public MyData createAccumulator() {
                                return new MyData(0L,System.currentTimeMillis(),0.0);
                            }

                            @Override
                            public MyData add(MyData value, MyData accumulator) {
                                return new MyData(value.count+accumulator.count,Math.min(value.timestamp,accumulator.timestamp),value.value+accumulator.value);
                            }

                            @Override
                            public MyData getResult(MyData accumulator) {
                                return accumulator;
                            }

                            @Override
                            public MyData merge(MyData a, MyData b) {
                                return null;
                            }
                        }
                )
                .sinkTo(kafkaSink);

        env.execute();
    }
}
