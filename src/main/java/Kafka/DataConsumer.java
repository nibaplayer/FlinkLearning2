package Kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class DataConsumer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<MyData> kafkaSource = KafkaSource.<MyData>builder()
                .setBootstrapServers("hadoop102:9092")
                .setTopics("demo")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new AbstractDeserializationSchema<MyData>() {
                    @Override
                    public MyData deserialize(byte[] message) throws IOException {
                        MyData ans = new MyData();
                        ans.Deserialization(message);
                        return ans;
                    }
                })
                .build();

        DataStreamSource<MyData> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),"kafka_source");

        stream.keyBy(r->r.getCount())
                .process(
                        new KeyedProcessFunction<Long, MyData, MyData>() {
                            ValueState<MyData> sumState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                sumState = getRuntimeContext().getState(new ValueStateDescriptor<MyData>("sumState", Types.POJO(MyData.class)));
                            }

                            @Override
                            public void processElement(MyData value, KeyedProcessFunction<Long, MyData, MyData>.Context ctx, Collector<MyData> out) throws Exception {
                                if(sumState.value()==null){
                                    sumState.update(new MyData(0L,System.currentTimeMillis(),0.0));
                                }
                                MyData temp =sumState.value();
                                temp.setCount(temp.getCount()+value.getCount());
                                temp.setValue(temp.getValue()+value.getValue());
                                temp.setTimestamp(Math.min(temp.getTimestamp(), value.timestamp));//时间用合并的所有数中最先产生的那个时间
                                sumState.update(temp);
                                if(temp.count>10000000L){//    计算1y亿个浮点数的和
                                    out.collect(temp);
                                    //todo 1
                                    throw new Exception((temp.count)+","+temp.timestamp+","+temp.value);//它接收到的可能不是这个异常
                                }
                            }
                        }
                )
                ;

        env.execute();
    }
}
