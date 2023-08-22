package Function;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;

public class MyHistogram implements Histogram {
    @Override
    public void update(long value) {

    }

    @Override
    public long getCount() {
        return 0;
    }

    @Override
    public HistogramStatistics getStatistics() {
        return null;
    }
}
