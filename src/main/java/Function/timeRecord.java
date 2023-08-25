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

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;

public class timeRecord {
    public static void main(String[] args) throws IOException {
        FileWriter fw = new FileWriter("Results.txt",true);
        int count=10;//进行几轮
        fw.write("窗口大小\t");
        for(int i=1;i<=count;i++){
            fw.write("第"+i+"轮\t");
        }
        fw.write("均值\t\n");
        //fw.write("窗口大小\t第一轮\t第二轮\t第三轮\t第四轮\t第五轮\t均值\t\n");
        //fw.flush();

        for(int i: new int[]{1,2,5,10,20,50,100,200,500,1000,2000,5000,10000,20000,50000,100000}){//窗口从1-100000
            fw.write((i)+"\t\t");
            Long total= 0L;
            for(int j=1;j<=count;j++){
                //每个跑count轮
                Long record = System.currentTimeMillis();
                try {
                    WindowsProcess(i);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    Long interval = System.currentTimeMillis()-record;
                    total+=interval;
                    fw.write((interval)+"\t");
                    System.out.println("窗口大小"+i+"第"+j+"轮Result:"+(interval));
                }
            }
            fw.write((Math.round(total*1.0/count))+"\t\n");
            System.out.println("窗口大小"+i+"平均Result:"+(Math.round(total/count)));
            fw.flush();
        }
        fw.close();
    }

    public static void WindowsProcess(int windows) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        DataGeneratorSource<MyNum> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, MyNum>() {
                    @Override
                    public MyNum map(Long value) throws Exception {
                        return new MyNum(1L,Math.random(),System.currentTimeMillis());
                    }
                },
                100000000L,//生成1亿个数
                RateLimiterStrategy.noOp(),
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
                .window(TumblingProcessingTimeWindows.of(Time.seconds(windows)))
                //这个窗也是作用在全局的  导致这步算子并行度变为1
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
                });//聚合
        KB.keyBy(new KeySelector<MyNum, Integer>() {
            @Override
            public Integer getKey(MyNum value) throws Exception {
                return 1;
            }
        }).process(new KeyedProcessFunction<Integer, MyNum, String>() {
            ValueState<MyNum> sumState;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                sumState = getRuntimeContext().getState(new ValueStateDescriptor<MyNum>("sumState",Types.POJO(MyNum.class)));
                //sumState.update(new MyNum(0L,0.0));
            }

            @Override
            public void processElement(MyNum value, KeyedProcessFunction<Integer, MyNum, String>.Context ctx, Collector<String> out) throws Exception {
                if(sumState.value()==null){
                    // 先初始化
                    sumState.update(new MyNum(0L,0.0));
                }
                MyNum temp = sumState.value();
                temp.setCount(temp.getCount()+value.getCount());
                temp.setValue(temp.getValue()+ value.getValue());
                sumState.update(temp);
//                if(temp.getCount()%1000000000L==0L){
//                    //输出一个
//                    out.collect("end of this turn!\nresult:"+temp.toString());
//
//                }
            }
        });

        env.execute();
    }
}

