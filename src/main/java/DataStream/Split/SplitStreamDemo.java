package DataStream.Split;


import DataStream.Aggregation.Reduce;
import Function.WaterSensor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SplitStreamDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("127.0.0.1", 7777)
                .map(new Reduce.WaterSensorFunction());
        //map将流里的数据变为了自定义的数据类型

        OutputTag<WaterSensor> s1 = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2 = new OutputTag<>("s2", Types.POJO(WaterSensor.class));

        SingleOutputStreamOperator<WaterSensor> process = ds.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value,
                                       ProcessFunction<WaterSensor, WaterSensor>.Context ctx,
                                       Collector<WaterSensor> out) throws Exception {
                if ("s1".equals(value.id)) {
                    ctx.output(s1, value);
                } else if ("s2".equals(value.id)) {
                    ctx.output(s2, value);
                } else {
                    out.collect(value);
                }
            }
        });
        process.print("主流");
        process.getSideOutput(s1).print("s1");
        process.getSideOutput(s2).print("s2");

        env.execute();
    }
}
