package SourceL;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        org.apache.flink.connector.file.src.FileSource<String> fileSource = org.apache.flink.connector.file.src.FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("input/test_phone_data.txt")).build();

        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(),"file").print();

        env.execute();
    }
}
