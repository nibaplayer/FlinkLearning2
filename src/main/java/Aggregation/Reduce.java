package Aggregation;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .socketTextStream("hadoop102",7777)
                .map(new WaterSensorFunction())
                .keyBy(WaterSensor::getId)
                .reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        System.out.print("reduce_demo");
                        int maxVc =Math.max(value1.getVc(),value2.getVc());
                        if(value1.getVc() > value2.getVc()){
                            value2.setVc(maxVc);
                            return value1;
                        }else{
                            value1.setVc(maxVc);
                            return value2;
                        }

                    }
                })
                .print();
        env.execute();
    }
    public static class WaterSensorFunction implements MapFunction<String, WaterSensor>{
        @Override
        public WaterSensor map(String value) throws Exception {
            String[] datas=value.split(",");
            return new WaterSensor(datas[0],Long.valueOf(datas[1]),Integer.valueOf(datas[2]));
        }
    }
}
