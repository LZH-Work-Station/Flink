package RichFunction;

import Reduce.Utils.StudentPojo;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class Test_01_richFunctionComparation {
    public static void main(String[] args) throws Exception {
        // 创建 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 执行环境并行度设置1
        env.setParallelism(1);

        StudentPojo s1 = new StudentPojo("LI ZEHAN", 18, 3, 90);
        StudentPojo s2 = new StudentPojo("JIA YIMEG", 20, 4, 90);
        StudentPojo s3 = new StudentPojo("ZHANG SAN", 34, 3, 80);
        StudentPojo s4 = new StudentPojo("LI SI", 59, 4, 100);

        DataStream<StudentPojo> dataStream = env.fromCollection(Arrays.asList(s1, s2, s3, s4));

        // 按照 班级分组
        KeyedStream<StudentPojo, Integer> keyedStream = dataStream.keyBy(student -> student.getClasses());

        // reduce出每个班级中的分数平均值, reduce要求进去啥类型出来也得啥类型
        DataStream<StudentPojo> averageGradeStream = keyedStream.reduce(new RichReduceFunction<StudentPojo>() {
            @Override
            public StudentPojo reduce(StudentPojo value1, StudentPojo value2) throws Exception {
                return new StudentPojo(value2.getClasses(), (value1.getGrade() + value2.getGrade()) / 2);
            }

            // 多了open和close方法，可以在打印日志的时候能提示开始和结束的信息
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("Open Reduce Operator");
            }

            @Override
            public void close() throws Exception {
                System.out.println("Close Reduce Operator");
            }
        });

        averageGradeStream.print("分数平均值");


        env.execute();
    }

}
