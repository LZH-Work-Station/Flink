package com.zehan.hotitems_analysis.UnitVisitor;

import com.typesafe.sslconfig.ssl.FakeChainedKeyStore;
import com.zehan.hotitems_analysis.beans.PageViewCount;
import com.zehan.hotitems_analysis.beans.UserBehavior;
import com.zehan.hotitems_analysis.beans.Uv;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;
import java.util.HashSet;
import java.util.Iterator;

public class UnitVisitorCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取数据，创建dataStream数据流
        DataStream<String> inputStream = env.readTextFile("src/main/resources/Data/UserBehavior.csv");

        // 3. 分配时间戳和watermark
        DataStream<UserBehavior> dataStream = inputStream
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            return new UserBehavior(Long.valueOf(fields[0]), Long.valueOf(fields[1]),
                                    Integer.valueOf(fields[2]), fields[3], Long.valueOf(fields[4]));
                        })
                // 分配水位线，允许200毫秒的延迟到达
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.milliseconds(200)) {
                    @Override
                    // 获取当前 事件的时间
                    public long extractTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        DataStream<Uv> uvCountStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .apply(new WindowApplyFunction());


        uvCountStream.print();

        env.execute();


    }

    public static class WindowApplyFunction implements AllWindowFunction<UserBehavior, Uv, TimeWindow> {

        @Override
        public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<Uv> out) throws Exception {
            // 定义一个Set结构，保存窗口中的所有userId，自动去重
            HashSet<Long> uidSet = new HashSet<>();
            Iterator it = values.iterator();
            while(it.hasNext()){
                UserBehavior ub = (UserBehavior) it.next();
                uidSet.add(ub.getUserId());
            }
            out.collect(new Uv("uv", window.getEnd(), (long) uidSet.size()));
        }
    }
}
