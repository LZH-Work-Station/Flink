package Reduce;

import Reduce.Utils.StudentPojo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class Test_04_Union {
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
        DataStream<StudentPojo> dataStream1 = env.fromCollection(Arrays.asList(s1, s2));
        DataStream<StudentPojo> dataStream2 = env.fromCollection(Arrays.asList(s3, s4));

        // Union可以 同时结合多个 数据流（数据流的数据结构必须相同）
        DataStream<StudentPojo> unionStream = dataStream1.union(dataStream2);

        unionStream.print("Union => ");

        env.execute();
    }
}
