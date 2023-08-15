package DataStream.Sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

public class SinkToFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);

        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        return "Number:"+value;
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(1000),
                Types.STRING
        );
        DataStreamSource<String> dataStreamSource = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-source");
        FileSink<String> fileSink = FileSink
                .<String>forRowFormat(new Path("D:\\code\\flink\\FlinkLearning\\output"), new SimpleStringEncoder<>("UTF-8"))
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("wangh")
                                .withPartSuffix(".log")
                                .build()
                )
                .withBucketAssigner(
                        new DateTimeBucketAssigner<>("yyyy-MM-dd HH", ZoneId.systemDefault())
                )
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(1))
                                .withMaxPartSize(new MemorySize(1024 * 1024))
                                .build()
                )
                .build();
        dataStreamSource.sinkTo(fileSink);
        env.execute();
    }
}
