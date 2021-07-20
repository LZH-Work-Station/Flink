package com.zehan.hotitems_analysis.hotitems;

import com.zehan.hotitems_analysis.beans.UserBehavior;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class HotItemsSql {
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

        // 4. 创建表执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 5. 将流转化成表
        Table dataTable = tableEnv.fromDataStream(
                dataStream, $("itemId"), $("behavior"), $("timestamp").rowtime().as("ts"));



        // 6. 分组开窗
        Table windowAggTable = dataTable
                // 过滤pv操作
                .filter($("behavior").isEqual("pv"))
                // 创建window
                .window(Slide.over("1.hours").every("5.minutes").on("ts").as("w"))
                // 按照itemId和window进行分组
                .groupBy($("itemId"), $("w"))
                // 提取itemId, windowEnd, 和item的数量
                .select($("itemId"), $("w").end().as("windowEnd"), $("itemId").count().as("cnt"));


        // 7. 利用开窗函数，对count值进行排序并获取row number得到Top N
        // SQL
        DataStream<Row> aggStream = tableEnv.toAppendStream(windowAggTable, Row.class);
        // 将aggStream转化成表并注册成agg
        tableEnv.createTemporaryView("agg", aggStream, $("itemId"), $("windowEnd"), $("cnt"));

        Table resultTable = tableEnv.sqlQuery("select * from " +
                // 开窗函数，对同一个窗口内的count进行排序
                "  ( select *, ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num " +
                "  from agg) " +
                " where row_num <= 5 ");

        tableEnv.toRetractStream(resultTable, Row.class).print();

        // 纯SQL实现
        tableEnv.createTemporaryView("data_table", dataStream, $("itemId"), $("behavior"), $("timestamp").rowtime().as("ts"));
        Table resultSqlTable = tableEnv.sqlQuery("select * from " +
                "  ( select *, ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num " +
                "  from ( " +
                "    select itemId, count(itemId) as cnt, HOP_END(ts, interval '5' minute, interval '1' hour) as windowEnd " +
                "    from data_table " +
                "    where behavior = 'pv' " +
                // HOP代表滑动窗口
                "    group by itemId, HOP(ts, interval '5' minute, interval '1' hour)" +
                "    )" +
                "  ) " +
                " where row_num <= 5 ");

        env.execute();
    }
}
