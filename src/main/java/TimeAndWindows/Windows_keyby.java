package TimeAndWindows;

import Demo.MyNum;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.runtime.rest.messages.job.metrics.Metric;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Windows_keyby {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(MetricOptions.REPORTERS_LIST,"stsd")
                .set(MetricOptions.REPORTER_INTERVAL, Duration.ofSeconds(10))
                .set(MetricOptions.REPORTER_FACTORY_CLASS,"TimeAndWindows.Windows_keyby")
                .set(MetricOptions.QUERY_SERVICE_PORT,"8125");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        //env.setParallelism(21);

        DataGeneratorSource<MyNum> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, MyNum>() {
                    @Override
                    public MyNum map(Long value) throws Exception {
                        return new MyNum(1L,Math.random());
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(1000000),
                Types.POJO(MyNum.class)

        );

        DataStreamSource<MyNum> ds = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");
        SingleOutputStreamOperator<MyNum> KB = ds.keyBy(new KeySelector<MyNum, Integer>() {//给每个mynum配一个随机的键值   这样他们会给随机的分到不同的分区
                    @Override
                    public Integer getKey(MyNum value) throws Exception {
                        return Integer.valueOf((int) (24 * value.getValue()));
                    }
                })
                .countWindow(10)
                .aggregate(new AggregateFunction<MyNum, MyNum, MyNum>() {
                    @Override
                    public MyNum createAccumulator() {
                        return new MyNum(0L, 0);
                    }

                    @Override
                    public MyNum add(MyNum value, MyNum accumulator) {
                        return new MyNum(value.getCount() + accumulator.getCount(), value.getValue() + accumulator.getValue());
                    }

                    @Override
                    public MyNum getResult(MyNum accumulator) {
                        return accumulator;
                    }

                    @Override
                    public MyNum merge(MyNum a, MyNum b) {
                        return null;
                    }
                });

//        KB.addSink(new SinkFunction<MyNum>() {
//            @Override
//            public void invoke(MyNum value, Context context) throws Exception {
//                SinkFunction.super.invoke(value, context);
//
//                value.myprint();
//            }
//        }).setParallelism(3);
        KB.process(
                new ProcessFunction<MyNum, String>() {
                    @Override
                    public void processElement(MyNum value, ProcessFunction<MyNum, String>.Context ctx, Collector<String> out) throws Exception {
                        StringBuilder outstr = new StringBuilder();
                        outstr.append(value.toString());
                        out.collect(outstr.toString());
                    }
                }
        ).setParallelism(3)

                .print().setParallelism(3);



        env.execute();
    }
}
