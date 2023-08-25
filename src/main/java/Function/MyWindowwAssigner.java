package Function;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Collection;

public class MyWindowwAssigner extends WindowAssigner<MyNum, Window> {
    @Override
    public Collection<Window> assignWindows(MyNum element, long timestamp, WindowAssignerContext context) {

        return null;
    }

    @Override
    public Trigger<MyNum, Window> getDefaultTrigger(StreamExecutionEnvironment env) {
        return null;
    }

    @Override
    public TypeSerializer<Window> getWindowSerializer(ExecutionConfig executionConfig) {
        return null;
    }

    @Override
    public boolean isEventTime() {
        return false;
    }
}
