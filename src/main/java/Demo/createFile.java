package Demo;

import org.apache.flink.FlinkVersion;
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

public class createFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        DataGeneratorSource<MyNum> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, MyNum>() {
                    @Override
                    public MyNum map(Long value) throws Exception {
                        return new MyNum(1L,Math.random());
                    }
                },
                100000000L,//生成1亿个数
                RateLimiterStrategy.noOp(),
                Types.POJO(MyNum.class)
        );
        DataStreamSource<MyNum> dataStreamSource = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-source");
        FileSink<MyNum> fileSink = FileSink
                .<MyNum>forRowFormat(new Path("D:\\code\\flink\\FlinkLearning\\output"), new SimpleStringEncoder<>("UTF-8"))
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
                                .withRolloverInterval(Duration.ofHours(2))
                                .withMaxPartSize(new MemorySize(1024 * 1024))
                                .build()
                )
                .build();
        dataStreamSource.sinkTo(fileSink);
        env.execute();
    }
}

