# 其他可选API

其他可选的API有点像 我们窗口的一个修饰流，用来修饰我们的窗口

```java
OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
};

SingleOutputStreamOperator<SensorReading> sumStream = dataStream.keyBy("id")
  .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))
  //                .trigger() // 触发器，一般不使用 
  //                .evictor() // 移除器，一般不使用
  .allowedLateness(Time.minutes(1)) // 允许迟到1分钟的结果。到了15s会立即得到一个结果，但是窗口不会关闭，等待1分钟，1分钟以内的数据也会被聚合。
  .sideOutputLateData(outputTag) // 侧输出流，迟到超过1分钟的数据，收集于此
  .sum("temperature"); // 侧输出流 对 温度信息 求和。

// 输出侧输出流中捕获的迟到1分钟的数据
sumStream.getSideOutput().print("late")
```

