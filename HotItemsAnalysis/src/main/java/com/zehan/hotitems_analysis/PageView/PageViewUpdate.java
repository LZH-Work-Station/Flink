package com.zehan.hotitems_analysis.PageView;

import com.zehan.hotitems_analysis.beans.Pv;
import com.zehan.hotitems_analysis.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

public class PageViewUpdate {

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
        DataStream<Pv> pvDataStream = dataStream.filter(data -> "pv".equals(data.getBehavior())) // 过滤pv行为
                // 如果只用一个Key = "pv"就会导致通过hash计算都被分配到同一个 分区里面 导致负载不均衡，所以我们采用随机数来改进
                .map(new MapFunction<UserBehavior, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> map(UserBehavior value) throws Exception {
                        // Tuple2类型必须使用 <>, 没有尖括号不行
                        Random random = new Random();
                        return new Tuple2<>(random.nextInt(10), 1L);
                    }
                })
                .keyBy(value -> value.f0)
                // 创建滚动时间窗口，窗口大小为1小时
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                // 这一步会自动按照 窗口进行求和
                .aggregate(new PvAggregate(), new PvWindowAggregate());

        SingleOutputStreamOperator<String> resultDataStream = pvDataStream.keyBy(new KeySelector<Pv, Long>() {
            @Override
            public Long getKey(Pv value) throws Exception {
                return value.getTimestamp();
            }
        }).process(new PvProcessFunction());

        resultDataStream.print();

        env.execute();
    }
    public static class PvAggregate implements AggregateFunction<Tuple2<Integer, Long>, Long, Long> {


        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Integer, Long> value, Long accumulator) {
            return accumulator + value.f1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    public static class PvWindowAggregate implements WindowFunction<Long, Pv, Integer, TimeWindow>{

        @Override
        public void apply(Integer s, TimeWindow window, Iterable<Long> input, Collector<Pv> out) throws Exception {
            out.collect(new Pv("pv", input.iterator().next(), window.getEnd()));
        }
    }

    public static class PvProcessFunction extends KeyedProcessFunction<Long, Pv, String> {
        // 也可以使用ValueState直接相加
        ListState<Long> pvCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            pvCountListState = getRuntimeContext().getListState(new ListStateDescriptor<Long>("pv-list", Long.class));
        }


        @Override
        public void processElement(Pv value, Context ctx, Collector<String> out) throws Exception {
            pvCountListState.add(value.getCount());
            ctx.timerService().registerEventTimeTimer(value.getTimestamp() + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Long sum = 0L;
            ArrayList<Long> list = Lists.newArrayList(pvCountListState.get().iterator());
            for(Long num : list) sum += num;

            StringBuilder sb = new StringBuilder();
            sb.append("=======================\n");
            sb.append("窗口结束时间").append(new Timestamp(timestamp - 1)).append("\n");

            sb.append("pv的总数：" + sum + "\n");
            sb.append("======================\n\n");

            out.collect(sb.toString());

        }
    }

}

