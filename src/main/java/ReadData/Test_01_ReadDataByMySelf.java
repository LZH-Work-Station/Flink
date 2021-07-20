package ReadData;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;


import java.util.HashMap;
import java.util.Random;

public class Test_01_ReadDataByMySelf {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Double>> dataStream = env.addSource(new MySensorSource());

        dataStream.print();

        env.execute();
    }

    // 实现自定义的SourceFunction
    public static class MySensorSource implements SourceFunction<Tuple2<String, Double>> {

        // 标示位，控制数据产生
        private volatile boolean running = true;


        @Override
        public void run(SourceContext<Tuple2<String, Double>> ctx) throws Exception {
            //定义一个随机数发生器
            Random random = new Random();

            // 设置10个传感器的初始温度
            HashMap<String, Double> DoubleMap = new HashMap<>();
            for (int i = 0; i < 10; ++i) {
                DoubleMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
            }

            while (running) {
                for (String sensorId : DoubleMap.keySet()) {
                    // 在当前温度基础上随机波动
                    Double newTemp = DoubleMap.get(sensorId) + random.nextGaussian();
                    DoubleMap.put(sensorId, newTemp);
                    ctx.collect(new Tuple2<>(sensorId, newTemp));
                }
                // 控制输出评率
                Thread.sleep(2000L);
            }
        }

        @Override
        public void cancel() {
            this.running = false;
        }
    }
}
