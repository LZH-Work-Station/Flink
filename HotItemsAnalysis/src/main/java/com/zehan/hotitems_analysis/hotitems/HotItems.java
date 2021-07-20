package com.zehan.hotitems_analysis.hotitems;

import com.zehan.hotitems_analysis.beans.ItemViewCount;
import com.zehan.hotitems_analysis.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

public class HotItems {
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
        DataStream<ItemViewCount> itemViewCountDataStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior())) // 过滤pv行为
                .keyBy(new KeySelector<UserBehavior, Long>() { // 按照itemID 分组
                    @Override
                    // 按照itemId进行分组
                    public Long getKey(UserBehavior value) throws Exception {
                        return value.getItemId();
                    }
                })
                // 创建时间窗口，窗口大小为1小时，每过5min统计一次
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                // aggregate(aggregateFunction, WindowAggreagateFunction)
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());

        // 5. 收集所有窗口内的count数据 排序输出top n
        DataStream<String> resultStream = itemViewCountDataStream
                .keyBy(new KeySelector<ItemViewCount, Long>(){
                    @Override
                    public Long getKey(ItemViewCount value) throws Exception {
                        return value.getWindowEnd();
                    }
                })
                .process(new TopNHotItems(5));

        resultStream.print();

        env.execute();

    }
    // AggregateFunction<IN, ACC, OUT>
    public static class ItemCountAgg implements  AggregateFunction<UserBehavior, Long, Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
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

    // WindowFunction<IN, OUT, KEY, W extends Window> 跟apply方法顺序不一样，有点妖
    public static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Long, TimeWindow>{
        @Override
        // apply(key, window, input, output)
        public void apply(Long aLong, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
             Long itemId = aLong;
             Long windowEnd = window.getEnd();
             Long count = input.iterator().next();
             out.collect(new ItemViewCount(itemId, windowEnd, count));
        }
    }

    // 实现自定义 KeyedProcessFunction
    public static class TopNHotItems extends KeyedProcessFunction<Long, ItemViewCount, String>{
        // 定义属性 TOPN的大小
        private Integer topSize;

        public TopNHotItems(Integer topSize) {
            this.topSize = topSize;
        }

        // 定义列表状态：保存当前窗口内所有输出的ItemViewCount
        ListState<ItemViewCount> itemViewCountListState;

        // 通过open获取上下文，进而获取前面的 itemViewCountListState
        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-view-count-list", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 每来一条数据就存入list中并注册定时器
            itemViewCountListState.add(value);
            // 因为flink底层是按照时间戳注册定时器，所以在同一个window中的数据会注册同一个定时器而不是反复注册
            // getWindowEnd() + 1在一个相同的窗口内永远不会到达，所以当没有Input的时候 自动触发 onTimer方法
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，将收集到的数据进行排序输出
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());
            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return -Long.compare(o1.getCount(), o2.getCount());
                }
            });
            // 将排名信息格式成String, 方便打印输出
            StringBuilder sb = new StringBuilder();
            sb.append("=======================\n");
            sb.append("窗口结束时间").append(new Timestamp(timestamp - 1)).append("\n");

            // 遍历列表取TOP n输出
            for(int i = 0; i < Math.min(topSize, itemViewCounts.size()); i++){
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                sb.append("No ").append(i+1).append(":")
                        .append(" 商品ID = ").append(currentItemViewCount.getItemId())
                        .append(" 热门度 = ").append(currentItemViewCount.getCount())
                        .append("\n");
            }
            sb.append("======================\n\n");

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(sb.toString());
            // 关闭之前的state记录
            itemViewCountListState.clear();
        }
    }
}
