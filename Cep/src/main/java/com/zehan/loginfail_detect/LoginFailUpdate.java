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
import java.util.Iterator;
/*
    乱序数据会导致 不会触发报警 因为比如两个fail中插入了一个sucess的数据，就会导致，
    这两个连续的fail不会被统计，因为 ListState被 clear了
 */
public class LoginFailUpdate {
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

        // 定义列表状态：保存当前窗口内所有输出的loginEventListState
        ListState<LoginEvent> loginEventListState;

        // 通过open获取上下文，进而获取前面的 loginEventListState
        @Override
        public void open(Configuration parameters) throws Exception {
            loginEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail-list-state", LoginEvent.class));
        }
        // 以登录的事件作为 判断报警的触发条件，不再注册定时器
        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
            // 判断登陆状态
            if("fail".equals(value.getLoginState())){
                // 1. 如果登录失败，获取状态之中的登录失败时间，判断是否有失败事件
                Iterator<LoginEvent> iterator = loginEventListState.get().iterator();
                if(iterator.hasNext()){
                    // 1.1 如果已经有 判断是否是2s之内的
                    // 获取已有的登录失败事件
                    LoginEvent firstFailEvent = iterator.next();
                    if(value.getTimestamp() - firstFailEvent.getTimestamp() <= 2){
                        Long userId = ctx.getCurrentKey();
                        Long firstFailTime = firstFailEvent.getTimestamp();
                        Long lastFailTime = value.getTimestamp();
                        String warning = "Login Fail Warning";
                        out.collect(new LoginFailWarning(userId, firstFailTime, lastFailTime, warning));
                    }
                    // 不管报警与否都要清空状态把最新的fail的事件输入
                    loginEventListState.clear();
                    loginEventListState.add(value);
                }else{
                    // 1.2 如果没有登陆失败，就直接存入
                    loginEventListState.add(value);
                }
            }else{
                // 成功的话 直接清空状态
                loginEventListState.clear();
            }
        }
    }
}