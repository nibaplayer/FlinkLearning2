package Function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.rest.messages.job.metrics.Metric;

public class MyMapper extends RichMapFunction<String,String> {
    private transient Counter counter;
    private transient int valueToExpose = 0;
    private transient Histogram histogram;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.counter=getRuntimeContext()
                .getMetricGroup()
                .counter("myConuter");

        getRuntimeContext()
                .getMetricGroup()
                .gauge("MyGuage", new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return valueToExpose;
                    }
                });
        this.histogram=getRuntimeContext()
                .getMetricGroup()
                .histogram("myHistogram",new MyHistogram());
    }

    @Override
    public String map(String value) throws Exception {
        this.counter.inc();
        valueToExpose++;
        this.histogram.update(Long.valueOf(value));
        return value;
    }
}
