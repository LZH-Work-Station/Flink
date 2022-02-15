package com.zehan.orderPay;

import com.zehan.Beans.OrderEvent;
import com.zehan.beans.ReceiptEvent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class TxPayMatchByJoin {
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
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> resultStream = orderEventStream
                .keyBy(OrderEvent::getTxId)
                .intervalJoin(receiptEventStream.keyBy(ReceiptEvent::getTxId))
                .between(Time.seconds(-3), Time.seconds(5))  // -3, 5 区间范围
                .process(new TxPayMatchDetectByJoin());
        // 因为通过join的function，如果没有在interval里面收集到两个流中有相同key的数据，就不会进入process function，也就不会产生侧输出流
        resultStream.print("matched-pays");
        //resultStream.getSideOutput(unmatchedPays).print("unmatched-pays");
        //resultStream.getSideOutput(unmatchedReceipts).print("unmatched-receipts");
        env.execute();
    }
    // 实现自定义ProcessJoinFunction
    public static class TxPayMatchDetectByJoin extends ProcessJoinFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>{
        @Override
        public void processElement(OrderEvent left, ReceiptEvent right, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            out.collect(new Tuple2<>(left, right));
            //if(left==null) ctx.output(unmatchedReceipts, right);
            //else if(right==null) ctx.output(unmatchedPays, left);

        }
    }
}
