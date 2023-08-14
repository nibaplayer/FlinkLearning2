package SourceL;

import Demo.MyNum;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class DatagenSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //env.setParallelism(2);
        //这个生成器在不同的并行算子中 10000会被拆分
        DataGeneratorSource<MyNum> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, MyNum>() {
                    @Override
                    public MyNum map(Long value) throws Exception {
                        return new MyNum(1L,Math.random());
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(10),
                Types.POJO(MyNum.class)
                //POJO要求有getter 与 setter
        );

        DataStreamSource<MyNum> ds = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");
        ds.addSink(new SinkFunction<MyNum>() {
            @Override
            public void invoke(MyNum value, Context context) throws Exception {

                value.myprint();
            }
        });

        env.execute();
    }
}
