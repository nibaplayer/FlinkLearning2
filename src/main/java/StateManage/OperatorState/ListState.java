package StateManage.OperatorState;

import Function.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class ListState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())
                .setParallelism(2);


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

        env.fromSource(dataGeneratorSource,WatermarkStrategy.noWatermarks(),"data-source")
                .map(new MyCountMapFunction())
                .print();
        env.execute();
    }

    //不同子任务的算子状态各自独立   当算子的并行度重新调整时  需要收集所有的算子状态再进行重新分配   由于这一特性 故算子状态没有value值类型
    private static class MyCountMapFunction implements MapFunction<WaterSensor,Long>,CheckpointedFunction{
        private Long count = 0L;
        private org.apache.flink.api.common.state.ListState<Long> state;
        @Override
        public Long map(WaterSensor value) throws Exception {
            return count++;
        }
        //将本地变量拷贝到算子状态中， 开启checkpoint时才会调用
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("snapshotState...");
            //清空算子状态
            state.clear();
            //将本地状态添加到算子状态中
            state.add(count);
        }

        //初始化本地变量  在程序启动或恢复时，从状态中把数据添加到本地变量  每个子任务调用一次
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("initializeState...");
            //利用上下文初始化算子状态
            state = context
                    .getOperatorStateStore()
                    .getListState(new ListStateDescriptor<Long>("state",Types.LONG));
            //从算子状态把数据拷贝到本地变量
            if(context.isRestored()){
                for(Long c: state.get()){
                    count += c;
                }
            }
        }
    }
}
