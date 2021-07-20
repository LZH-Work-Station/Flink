package Transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test_Map {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使得任务抢占同一个线程
        env.setParallelism(1);

        // 从文件中获取数据输出
        DataStream<String> dataStream = env.readTextFile("src/main/resources/hello.txt");

        // 1. map, String => 字符串长度INT
        /*
            泛型的第一个是input的类型，泛型的第二个是output的类型
         */
        DataStream<String> mapStream = dataStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value.split(" ")[0];
            }
        });

        mapStream.print();

        env.execute();
    }
}
