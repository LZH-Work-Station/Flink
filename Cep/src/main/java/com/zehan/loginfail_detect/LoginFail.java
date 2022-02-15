package com.zehan.loginfail_detect;

import com.zehan.Beans.LoginEvent;
import com.zehan.Beans.LoginFailWarning;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/*
    需要增加时效性，我们的onTimer 不应该等到2s才触发，而是应该出现多次失败就报警
 */
public class LoginFail {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取数据，创建dataStream数据流
        DataStream<LoginEvent> loginEventStream =
                env.readTextFile("D:/Tutorial/Flink/HotItemsAnalysis/src/main/resources/Data/LoginLog.csv")
                    .map( line -> {
                        String[] fields = line.split(",");
                        return new LoginEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                    })
                    .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                            new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(3)) {
                                @Override
                                public long extractTimestamp(LoginEvent element) {
                                    return element.getTimestamp();
                                }
                            }
                    ));

        // 自定义处理函数检测连续登录失败事件
        SingleOutputStreamOperator<LoginFailWarning> warningStream = loginEventStream
                .keyBy(event -> event.getUserId())
                .process(new LoginFailDetectWarning(3L));

        warningStream.print();

        env.execute("login fail detect job");


    }

    public static class LoginFailDetectWarning extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning>{
        // 定义报警阈值
        private Long warningThreshold;

        public LoginFailDetectWarning(Long warningThreshold) {
            this.warningThreshold = warningThreshold;
        }

        // 定义列表状态：保存当前窗口内所有输出的ItemViewCount
        ListState<LoginEvent> loginEventListState;
        ValueState<Long> timestampValueState;

        // 通过open获取上下文，进而获取前面的 itemViewCountListState
        @Override
        public void open(Configuration parameters) throws Exception {
            loginEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail-list-state", LoginEvent.class));
            timestampValueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timestamp-value-state", Long.class));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
            // 来了数据，如果是Fail就加入List
            if(value.getLoginState().equals("fail")) {
                loginEventListState.add(value);
                // 判断有没有定时器
                if (timestampValueState.value() == null) {
                    // 没有定时器 我们就依照现在的timestamp创建定时器
                    Long ts = value.getTimestamp() * 1000L + 2000L;
                    ctx.timerService().registerEventTimeTimer(ts);
                    timestampValueState.update(ts);
                }
                // 如果是正确登录，我们要删除定时器并且清空ListState
            }else{
                if(timestampValueState.value()!=null) {
                    ctx.timerService().deleteEventTimeTimer(timestampValueState.value());
                    loginEventListState.clear();
                    timestampValueState.clear();
                }
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<LoginFailWarning> out) throws Exception {
            ArrayList<LoginEvent> loginEventArrayList = Lists.newArrayList(loginEventListState.get().iterator());
            // 如果大于最大值就报警
            if(loginEventArrayList.size() >= warningThreshold){
                Long userId = ctx.getCurrentKey();
                Long firstFailTime = loginEventArrayList.get(0).getTimestamp();
                Long lastFailTime = loginEventArrayList.get(loginEventArrayList.size() - 1).getTimestamp();
                String warning = "Login Fail Warning";
                out.collect(new LoginFailWarning(userId, firstFailTime, lastFailTime, warning));
            }
            // 清除状态
            loginEventListState.clear();
            timestampValueState.clear();
        }
    }
}