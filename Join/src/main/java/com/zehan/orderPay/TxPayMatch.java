package com.zehan.orderPay;

import com.zehan.Beans.OrderEvent;
import com.zehan.beans.ReceiptEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class TxPayMatch {
    private final static OutputTag<OrderEvent> unmatchedPays = new OutputTag<OrderEvent>("unmatched-pays"){};
    private final static OutputTag<ReceiptEvent> unmatchedReceipts = new OutputTag<ReceiptEvent>("unmatched-receipts"){};

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取数据，创建dataStream数据流
        DataStream<OrderEvent> orderEventStream =
                env.readTextFile("D:/Tutorial/Flink/HotItemsAnalysis/src/main/resources/Data/OrderLog.csv")
                        .map(line -> {
                            String[] fields = line.split(",");
                            return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                        })
                        .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                                new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.seconds(1)) {
                                    @Override
                                    public long extractTimestamp(OrderEvent element) {
                                        return element.getTimestamp() * 1000L;
                                    }
                                }
                        ))
                        .filter(data -> !"".equals(data.getTxId()));

        DataStream<ReceiptEvent> receiptEventStream =
                env.readTextFile("D:/Tutorial/Flink/HotItemsAnalysis/src/main/resources/Data/ReceiptLog.csv")
                        .map(line -> {
                            String[] fields = line.split(",");
                            return new ReceiptEvent(fields[0], fields[1], new Long(fields[2]));
                        })
                        .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                                new BoundedOutOfOrdernessTimestampExtractor<ReceiptEvent>(Time.seconds(1)) {
                                    @Override
                                    public long extractTimestamp(ReceiptEvent element) {
                                        return element.getTimestamp() * 1000L;
                                    }
                                }
                        ));

        // 将两条流进行连接合并，进行匹配处理
        // 不匹配的数据放进侧输出流标签

        // 因为进行了keyby，所以在connectStream里面只有最多两个值，一个是支付，另一个是付款。所以下面用ValueState就足够了
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> resultStream = orderEventStream.keyBy(OrderEvent::getTxId)
                .connect(receiptEventStream.keyBy(ReceiptEvent::getTxId))
                .process(new TxPayMatchDetect());
        resultStream.print("matched-pays");
        resultStream.getSideOutput(unmatchedPays).print("unmatched-pays");
        resultStream.getSideOutput(unmatchedReceipts).print("unmatched-receipts");

        env.execute();
    }

    // 实现自定义coProcessFunction
    public static class TxPayMatchDetect extends CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>{
        // 定义状态，保存当前到来的订单支付事件和到账事件
        ValueState<OrderEvent> payState;
        ValueState<ReceiptEvent> receiptState;

        @Override
        public void open(Configuration parameters) throws Exception {
            payState = getRuntimeContext().getState((new ValueStateDescriptor<OrderEvent>("pay", OrderEvent.class)));
            receiptState = getRuntimeContext().getState((new ValueStateDescriptor<ReceiptEvent>("receipt", ReceiptEvent.class)));
        }

        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            // 订单支付事件来了
            ReceiptEvent receipt = receiptState.value();
            if(receipt != null){
                // 如果receipt不为空，说明到账事件已经来过 输出匹配事件，清空状态
                out.collect(new Tuple2<>(value, receipt));
                payState.clear();
                receiptState.clear();
            }else{
                // 如果receipt没来，注册一个定时器，等待
                ctx.timerService().registerEventTimeTimer((value.getTimestamp() + 5)*1000L);
                // 更新状态
                payState.update(value);
            }
        }

        @Override
        public void processElement2(ReceiptEvent value, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            // 到账事件来了，判断是否有支付事件
            OrderEvent pay = payState.value();
            if(pay != null){
                // 如果receipt不为空，说明到账事件已经来过 输出匹配事件，清空状态
                out.collect(new Tuple2<>(pay, value));
                payState.clear();
                receiptState.clear();
            }else{
                // 如果receipt没来，注册一个定时器，等待
                ctx.timerService().registerEventTimeTimer((value.getTimestamp() + 5)*1000L);
                // 更新状态
                receiptState.update(value);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            // 定时器触发，有可能是有一个没来，也有可能 是来了，但是因为我们刚才没清空定时器
            // 判断哪个不为空，那么另一个就没来
            if(payState.value()!=null){
                ctx.output(unmatchedPays, payState.value());
                payState.clear();
            }
            if(receiptState.value()!=null){
                ctx.output(unmatchedReceipts, receiptState.value());
                receiptState.clear();
            }



        }
    }
}
