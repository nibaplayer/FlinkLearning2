package StateManage.KeyedState;

import Function.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
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

public class StateTTL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataGeneratorSource<WaterSensor> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, WaterSensor>() {
                    @Override
                    public WaterSensor map(Long value) throws Exception {
                        return new WaterSensor("s"+(int)(10*Math.random()), System.currentTimeMillis(),(int)(10*Math.random()));
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(20),
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
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    ValueState<Integer> lastVcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        //TODO 1.创建stateTllconfig
                        StateTtlConfig stateTtlConfig = StateTtlConfig
                                .newBuilder(Time.seconds(5))//5秒过期
                                //.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)   创建和写入是更新过期时间
                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)// 创建 读取 写入 都更新过期时间
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)//不返回过期的状态
                                .build();
                        //TODO 2.创建状态描述器  启用TLL
                        ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("lastVcState",Types.INT);
                        stateDescriptor.enableTimeToLive(stateTtlConfig);
                        this.lastVcState= getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        Integer lastVc = lastVcState.value();
                        out.collect("key="+value.getId()+",状态值"+lastVc);
                        if(value.getVc()>5){
                            lastVcState.update(value.getVc());
                        }

                    }
                }).print();
        env.execute();
    }
}
