package Window;

import Reduce.Utils.StudentPojo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;

public class Test {
    public static void main(String[] args) {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置env并行度1，使得整个任务抢占同一个线程执行
        env.setParallelism(8);

        StudentPojo s1 = new StudentPojo("LI ZEHAN", 18, 3);
        StudentPojo s2 = new StudentPojo("JIA YIMEG", 20, 4);
        StudentPojo s3 = new StudentPojo("ZHANG SAN", 34, 3);
        StudentPojo s4 = new StudentPojo("LI SI", 59, 4);

        // Source: 从集合Collection中获取数据
        DataStream<StudentPojo> dataStream = env.fromCollection(Arrays.asList(s1, s2, s3, s4));
        OutputTag<StudentPojo> outputTag = new OutputTag<StudentPojo>("late") {
        };

        SingleOutputStreamOperator<Integer> apply = dataStream.keyBy(StudentPojo -> StudentPojo.getClasses())
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .apply(new WindowFunction<StudentPojo, Integer, Integer, TimeWindow>() {
                    @Override
                    public void apply(Integer integer, TimeWindow window, Iterable<StudentPojo> input, Collector<Integer> out) throws Exception {

                    }
                });

        apply.getSideOutput(outputTag);
    }
}
