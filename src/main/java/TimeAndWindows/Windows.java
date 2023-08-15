package TimeAndWindows;

import Demo.MyNum;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.nio.channels.NotYetBoundException;

public class Windows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        //这个生成器在不同的并行算子中 10000会被拆分
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

        SingleOutputStreamOperator<MyNum> aggregate = ds.countWindowAll(2).aggregate(new AggregateFunction<MyNum, MyNum, MyNum>() {
            //windowall将并行度变为了1 全局的窗口
            @Override
            public MyNum createAccumulator() {
                //System.out.println("create acc");
                return new MyNum(0L, 0.0);
            }

            @Override
            public MyNum add(MyNum value, MyNum accumulator) {
                //System.out.println("adding");
                return new MyNum(value.getCount() + accumulator.getCount(), value.getValue() + value.getValue());
            }

            @Override
            public MyNum getResult(MyNum accumulator) {
                //System.out.println("get result");
                return accumulator;
            }

            @Override
            public MyNum merge(MyNum a, MyNum b) {
                //System.out.println("merging");
                return null;
            }
        });
        //开一个容量为2的窗口   二合一
        SingleOutputStreamOperator<MyNum> aggregate1 = aggregate.countWindowAll(2).aggregate(new AggregateFunction<MyNum, MyNum, MyNum>() {
            @Override
            public MyNum createAccumulator() {
                //System.out.println("create acc");
                return new MyNum(0L, 0.0);
            }

            @Override
            public MyNum add(MyNum value, MyNum accumulator) {
                //System.out.println("adding");
                return new MyNum(value.getCount() + accumulator.getCount(), value.getValue() + value.getValue());
            }

            @Override
            public MyNum getResult(MyNum accumulator) {
                //System.out.println("get result");
                return accumulator;
            }

            @Override
            public MyNum merge(MyNum a, MyNum b) {
                //System.out.println("merging");
                return null;
            }
        });
        //多次聚合
        aggregate1.addSink(new SinkFunction<MyNum>() {
            @Override
            public void invoke(MyNum value, Context context) throws Exception {
                value.myprint();
            }
        });


        env.execute();
    }
}
