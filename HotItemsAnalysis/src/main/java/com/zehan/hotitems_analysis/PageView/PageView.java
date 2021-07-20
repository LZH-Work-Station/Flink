package com.zehan.hotitems_analysis.PageView;

import com.zehan.hotitems_analysis.beans.ItemViewCount;
import com.zehan.hotitems_analysis.beans.UserBehavior;
import com.zehan.hotitems_analysis.hotitems.HotItems;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;

public class PageView {
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


        // 4. 分组开窗聚合，得到每个窗口内每个商品的count值
        DataStream<Tuple2<String, Long>> pvResultDataStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior())) // 过滤pv行为
                // 由于没有什么好keyBy的，所以就在前面加上同一个字段作为Key，也不用分组，只是为了使用aggregate的api
                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                        // Tuple2类型必须使用 <>, 没有尖括号不行
                        return new Tuple2<>("pv", 1L);
                    }
                })
                .keyBy(value -> value.f0)
                // 创建滚动时间窗口，窗口大小为1小时
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                // 这一步会自动按照 窗口进行求和
                // 仅仅使用这种方式无法获得PV窗口的信息，不是很棒，还是得用aggregate
                .sum(1);

        pvResultDataStream.print();

        env.execute();


    }
}
