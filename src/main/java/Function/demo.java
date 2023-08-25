package Function;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //env.setParallelism(2);
        //env.socketTextStream()
        DataGeneratorSource<MyNum> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, MyNum>() {
                    @Override
                    public MyNum map(Long value) throws Exception {
                        return new MyNum(1L,Math.random(),System.currentTimeMillis());
                    }
                },
                Long.MAX_VALUE,//生成10亿个数
                RateLimiterStrategy.perSecond(10000),
                Types.POJO(MyNum.class)
        );

        SingleOutputStreamOperator<MyNum> ds = env
                .fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<MyNum>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, ts) -> element.getTs())
                )
                .setParallelism(3);

        SingleOutputStreamOperator<MyNum> KB = ds.keyBy(new KeySelector<MyNum, Integer>() {//给每个mynum配一个随机的键值   这样他们会给随机的分到不同的分区
                    @Override
                    public Integer getKey(MyNum value) throws Exception {
                        return (int)(Math.random()*3);
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
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
                        return new MyNum(a.getCount()+b.getCount(),a.getValue()+b.getValue());
                    }
                })
                .setParallelism(3);//十合一
        //KB.print();
        //KB.slotSharingGroup()
        KB.keyBy(new KeySelector<MyNum, Integer>() {
            @Override
            public Integer getKey(MyNum value) throws Exception {
                return 1;
            }
        }).process(new KeyedProcessFunction<Integer, MyNum, MyNum>() {
            ValueState<MyNum> sumState;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                sumState = getRuntimeContext().getState(new ValueStateDescriptor<MyNum>("sumState",Types.POJO(MyNum.class)));
                //sumState.update(new MyNum(0L,0.0));
            }

            @Override
            public void processElement(MyNum value, KeyedProcessFunction<Integer, MyNum, MyNum>.Context ctx, Collector<MyNum> out) throws Exception {
                if(sumState.value()==null){
                    // 先初始化
                    sumState.update(new MyNum(0L,0.0));
                }
                MyNum temp = sumState.value();
                temp.setCount(temp.getCount()+value.getCount());
                temp.setValue(temp.getValue()+ value.getValue());
                sumState.update(temp);
                if(temp.getCount()>10000L){
                    //每1万输出一个
                    out.collect(temp);
                }
            }
        })
                .setParallelism(1)
                .print()
                .setParallelism(1);

        env.execute();

    }
}
