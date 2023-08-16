package StateManage;

import Function.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class ValueState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataGeneratorSource<WaterSensor> dataGeneratorSource= new DataGeneratorSource<>(
                new GeneratorFunction<Long, WaterSensor>() {
                    @Override
                    public WaterSensor map(Long value) throws Exception {
                        return new WaterSensor("s"+(int)(10*Math.random()), System.currentTimeMillis(),(int)(15*Math.random()));
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(10),
                Types.POJO(WaterSensor.class)
        );
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-source")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, ts) -> element.getTs())
                );

        sensorDS.keyBy(r->r.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            org.apache.flink.api.common.state.ValueState<Integer> lastVcState;
                            @Override
                            public void open(Configuration parameters) throws Exception {//初始化状态

                                super.open(parameters);
                                lastVcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVcState",Types.INT));

                            }

                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                                int lastVc = lastVcState.value() == null ? 0: lastVcState.value();
                                //这个状态就像一个局部变量一样   可以在这条keyedstream中随时取用
                                Integer vc = value.getVc();
                                if(Math.abs(vc - lastVc)>10){
                                    out.collect("传感器="+value.getId()+"==>当前水位="+vc+",与上一条水位值="+lastVc+",相差超过10！！！");
                                }

                                //更新状态
                                lastVcState.update(vc);
                            }
                        }
                )
                .print();
        env.execute();
    }
}
