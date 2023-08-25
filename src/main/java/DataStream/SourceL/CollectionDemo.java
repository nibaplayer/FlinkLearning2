package DataStream.SourceL;

import org.apache.commons.collections.ArrayStack;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class CollectionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Integer> data = Arrays.asList(1,22,3);

        DataStreamSource<Integer> ds = env.fromCollection(data);

        //env.socketTextStream()
        ds.print();

        env.execute();
    }
}
