package DataStream.Transform;

import DataStream.SourceL.DatagenSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyPartitioner {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

            //env.setParallelism(2);

        DataGeneratorSource<Long> longDataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, Long>() {
                    @Override
                    public Long map(Long value) throws Exception {
                        return (long) (100 * Math.random());
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(10),
                Types.LONG
        );//随机生成0-99的一个数

        env.fromSource(longDataGeneratorSource, WatermarkStrategy.noWatermarks(),"long-generator")
                .keyBy(new KeySelector<Long, Long>() {
                    @Override
                    public Long getKey(Long value) throws Exception {
                        return value;
                    }
                })
                .print();

//            DataStream<String> myDS = socketDS
//                    .partitionCustom(
//                            new Partitioner<String>() {
//                                @Override
//                                public int partition(String key, int numPartitions) {
//                                    return 0;
//                                }
//                            },
//                            value -> value);
//
//
//            myDS.print();

            env.execute();
        }

}
