package ReadData;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class Test_02_ReadDataFromCollection {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置env并行度1，使得整个任务抢占同一个线程执行
        env.setParallelism(4);

        // Source: 从集合Collection中获取数据
        DataStream<Tuple2> dataStream = env.fromCollection(
                Arrays.asList(
                        new Tuple2("sensor_1", 1),
                        new Tuple2("sensor_6", 2),
                        new Tuple2("sensor_7", 3),
                        new Tuple2("sensor_10", 4)
                )
        );

        DataStream<Integer> intStream = env.fromElements(1,2,3,4,5,6,7,8,9);

        // 打印输出
        // 后面的String是给print的数据前面加注释
        dataStream.print("Tuple");
        intStream.print("INT");

        // 执行
        env.execute("JobName");

    }
}
