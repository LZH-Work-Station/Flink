package Reduce;

import Reduce.Utils.StudentPojo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class Test_01_KeyBy {
    public static void main(String[] args) throws Exception {
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

        DataStream<StudentPojo> keyedStream = dataStream.keyBy(new KeySelector<StudentPojo, Integer>() {
            @Override
            public Integer getKey(StudentPojo value) throws Exception {
                return value.getClasses();
            }
        });



        keyedStream.print();
        env.execute();


    }


}

