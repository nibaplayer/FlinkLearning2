package TimeAndWindows;

import Demo.MyNum;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class Windows_keyby {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //env.setParallelism(21);
        //这个生成器在不同的并行子任务中 10000会被拆分
        DataGeneratorSource<MyNum> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, MyNum>() {
                    @Override
                    public MyNum map(Long value) throws Exception {
                        return new MyNum(1L,Math.random());
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(100),
                Types.POJO(MyNum.class)

        );

        DataStreamSource<MyNum> ds = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");
        KeyedStream<MyNum, Integer> KB = ds.keyBy(new KeySelector<MyNum, Integer>() {//给每个mynum配一个随机的键值   这样他们会给随机的分到不同的分区
            @Override
            public Integer getKey(MyNum value) throws Exception {
                return Integer.valueOf((int) (24* value.getValue()));
            }
        });
        KB.addSink(new SinkFunction<MyNum>() {
            @Override
            public void invoke(MyNum value, Context context) throws Exception {
                SinkFunction.super.invoke(value, context);
                value.myprint();
            }
        });

        env.execute();
    }
}
