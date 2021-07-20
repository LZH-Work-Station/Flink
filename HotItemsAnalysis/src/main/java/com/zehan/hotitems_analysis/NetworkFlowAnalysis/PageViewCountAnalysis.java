package com.zehan.hotitems_analysis.NetworkFlowAnalysis;

import com.zehan.hotitems_analysis.beans.ApacheLogEvent;
import com.zehan.hotitems_analysis.beans.ItemViewCount;
import com.zehan.hotitems_analysis.beans.PageViewCount;
import com.zehan.hotitems_analysis.hotitems.HotItems;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.tools.nsc.doc.html.Page;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.regex.Pattern;

public class PageViewCountAnalysis {
    private String line;

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取数据，创建dataStream数据流
        DataStream<String> inputStream = env.readTextFile("src/main/resources/Data/apache.log");



        SingleOutputStreamOperator<ApacheLogEvent> dataStream = inputStream.map(
                line -> {
                    String[] fields = line.split(" ");
                    // 按照String的格式定义类型
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                    // 日期转化成时间戳(毫秒级别) 下面不用 * 1000L
                    Long timestamp = simpleDateFormat.parse(fields[3]).getTime();

                    return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(ApacheLogEvent element) {
                        return element.getTimestamp();
                    }
                });
        // 侧输出流，必须使用匿名内部类，必须有花括号
        OutputTag<ApacheLogEvent> lateTag = new OutputTag<ApacheLogEvent>("late"){};
        // 分组开窗聚合
        SingleOutputStreamOperator<com.zehan.hotitems_analysis.beans.PageViewCount> aggStream = dataStream
                .filter(data -> "GET".equals(data.getMethode()))
                .filter(data -> {
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
                })
                .keyBy(new KeySelector<ApacheLogEvent, String>() {
                    @Override
                    public String getKey(ApacheLogEvent value) throws Exception {
                        return value.getUrl();
                    }
                })
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
                // 由于普遍延迟过高，所以我们设置允许迟到
                .allowedLateness(Time.seconds(5))
                .sideOutputLateData(lateTag)
                .aggregate(new myAggregate(), new windowAggregate());

        aggStream.getSideOutput(lateTag).print("late");

        // 收集同一个窗口count数据，排序输出
        // 按照windowEnd分组

        aggStream.keyBy(new KeySelector<com.zehan.hotitems_analysis.beans.PageViewCount, Long>() {

            @Override
            public Long getKey(com.zehan.hotitems_analysis.beans.PageViewCount value) throws Exception {
                return value.getWindowEnd();
            }
        })
                .process(new TopNHotPage(5)).print();


        env.execute();
    }

    public static class myAggregate implements AggregateFunction<ApacheLogEvent, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent value, Long accumulator) {
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

    public static class windowAggregate implements WindowFunction<Long, com.zehan.hotitems_analysis.beans.PageViewCount, String, TimeWindow>{

        @Override
        public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<com.zehan.hotitems_analysis.beans.PageViewCount> out) throws Exception {
            Long count = input. iterator().next();
            Long windowEnd = window.getEnd();

            out.collect(new com.zehan.hotitems_analysis.beans.PageViewCount(s, windowEnd, count));
        }
    }

    public static class TopNHotPage extends KeyedProcessFunction<Long, com.zehan.hotitems_analysis.beans.PageViewCount, String> {
        private Integer topSize;

        public TopNHotPage(Integer size){
            this.topSize = size;
        }
        // 定义状态 存放所有PageViewCount到List中
        private MapState<String, Long> pageViewCountMapState;

        @Override
        // 获取 ListState
        public void open(Configuration parameters) throws Exception {
            pageViewCountMapState = getRuntimeContext().
                    getMapState(new MapStateDescriptor<String, Long>("page-count-map", String.class, Long.class));
        }

        @Override
        public void processElement(com.zehan.hotitems_analysis.beans.PageViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 使用
            pageViewCountMapState.put(value.getUrl(), value.getCount());
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 60 * 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 先判断是否到了窗口关闭清理时间，如果是，清空状态返回
            // getCurrentKey获得的是 做window操作之前的keyBy的key，在这里是windowEnd
            if(timestamp == ctx.getCurrentKey() + 60 * 1000L){
                pageViewCountMapState.clear();
                return;
            }
            /*
                采用Map的原因是，因为我们是 乱序数据，为了保证能够实时输出，所以我们会先输出，然后每有一个迟到的数据就更新一次报告。
                所以，如果我们继续使用List，就会插入一条新的count记录，所以在整个pageViewCounts中，就会产生 多条有相同URL的记录，
                只不过每次加1。

                所以我们采用Map的形式进行更新，每来一条新数据，我们不会去添加新纪录，而是在旧的记录上更新。所以每有一条迟到数据，就会
                触发一次onTimer?
             */
            ArrayList<Map.Entry<String, Long>> pageViewCounts = Lists.newArrayList(pageViewCountMapState.entries().iterator());
            pageViewCounts.sort(new Comparator<Map.Entry<String, Long>>() {
                @Override
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                    if(o1.getValue() > o2.getValue()) return -1;
                    else if(o1.getValue() < o2.getValue()) return 1;
                    else return 0;
                }
            });

            // 将排名信息格式成String, 方便打印输出
            StringBuilder sb = new StringBuilder();
            sb.append("=======================\n");
            sb.append("窗口结束时间").append(new Timestamp(timestamp - 1)).append("\n");

            // 遍历列表取TOP n输出
            for(int i = 0; i < Math.min(topSize, pageViewCounts.size()); i++){
                Map.Entry<String, Long> currentItemViewCount = pageViewCounts.get(i);
                sb.append("No ").append(i+1).append(":")
                        .append(" URL = ").append(currentItemViewCount.getKey())
                        .append(" 热门度 = ").append(currentItemViewCount.getValue())
                        .append("\n");
            }
            sb.append("======================\n\n");

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(sb.toString());

        }
    }
}
