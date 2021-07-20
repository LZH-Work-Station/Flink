package Reduce;
import Reduce.Utils.StudentPojo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class Test_02_RollingAggregation {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置env并行度1，使得整个任务抢占同一个线程执行
        env.setParallelism(1);

        StudentPojo s1 = new StudentPojo("LI ZEHAN", 18, 3);
        StudentPojo s2 = new StudentPojo("JIA YIMEG", 20, 4);
        StudentPojo s3 = new StudentPojo("ZHANG SAN", 34, 3);
        StudentPojo s4 = new StudentPojo("LI SI", 59, 4);

        // Source: 从集合Collection中获取数据
        DataStream<StudentPojo> dataStream = env.fromCollection(Arrays.asList(s1, s2, s3, s4));

        KeyedStream<StudentPojo, Integer> keyedStream = dataStream.keyBy(
                (KeySelector<StudentPojo, Integer>) value -> value.getClasses());

        /**
         *   必须要先分组再聚合，然后无论是max还是sum都是算的各自分组里面的
         *   对 keyedStream做找最大年龄的 rolling aggreagte 还有
         *   sum, max, min, maxBy, minBy
         *   max的话 只有被比较的字段会改变，比如下面的名字 班级都不会改变 但是年龄会改变
         *   maxBy的话 整个都会改变
         */

        DataStream<StudentPojo> max = keyedStream.max("age");
        DataStream<StudentPojo> maxBy = keyedStream.maxBy("age");
        max.print("======================MAX Test=======================");
        maxBy.print("======================MAXBY Test=======================");


        env.execute();
    }
}