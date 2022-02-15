package com.zehan.loginfail_detect;

import com.zehan.Beans.LoginEvent;
import com.zehan.Beans.LoginFailWarning;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;

import java.util.List;
import java.util.Map;

public class LoginFailCep {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取数据，创建dataStream数据流
        DataStream<LoginEvent> loginEventStream =
                env.readTextFile("D:/Tutorial/Flink/HotItemsAnalysis/src/main/resources/Data/LoginLog.csv")
                        .map(line -> {
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


        // 1. 定义一个匹配模式，定义一个模式，然后如果符合这个模式 就是被检测出来
        // firstFail -> secondFail, within 2s
        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern.<LoginEvent>begin("firstFail").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return value.getLoginState().equals("fail");
            }
        })
        // next代表下一个 fail的情况，next在上一个fail的事件，在两秒内
                .next("secondFail").where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.getLoginState().equals("fail");
                    }
                })
                .within(Time.seconds(10));

        // 2. 将匹配模式应用到数据流上，得到一个pattern stream
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStream
                        .keyBy(LoginEvent::getUserId)
                , loginFailPattern);

        // 3. 检出符合匹配条件的复杂事件，进行 转换处理，得到报警信息。
        SingleOutputStreamOperator<LoginFailWarning> warningStream = patternStream.select(new LoginFailMathcDetectWarning());

        warningStream.print();

        env.execute("login fail detect job");
    }


    public static class LoginFailMathcDetectWarning implements PatternSelectFunction<LoginEvent, LoginFailWarning>{
        @Override
        /***
         * @param Map<String, List<LoginEvent>> String 是我们定义的pattern里面的名字，例如firstFail;
         * @param List<LoginEvent> 是我们filter return出来的被匹配的LoginEvent
         *
         */
        public LoginFailWarning select(Map<String, List<LoginEvent>> pattern) throws Exception {
            LoginEvent l1 = pattern.get("firstFail").get(0);
            LoginEvent l2 = pattern.get("secondFail").get(0);

            Long userId = l1.getUserId();
            Long firstFailTime = l1.getTimestamp();
            Long lastFailTime = l2.getTimestamp();
            String warning = "Login Fail Warning";
            return new LoginFailWarning(userId, firstFailTime, lastFailTime, warning);
        }
    }
}
