# Group Windows

- Group Windows 是使用 window（w:GroupWindow）子句定义的，并且**必须由as子句指定一个别名**。

- 为了按窗口对表进行分组，窗口的别名必须在 group by 子句中，像常规的分组字段一样引用

  ```scala
  Table table = input
  .window([w:GroupWindow] as "w") // 定义窗口，别名为w
  .groupBy("w, a") // 按照字段 a和窗口 w分组
  .select("a,b.sum"); // 聚合
  ```

- Table API 提供了一组具有特定语义的预定义 Window 类，这些类会被转换为底层 DataStream 或 DataSet 的窗口操作

- 分组窗口分为三种：

  - 滚动窗口
  - 滑动窗口
  - 会话窗口

  

#### [滚动窗口(Tumbling windows)](https://ashiamd.github.io/docsify-notes/#/study/BigData/Flink/尚硅谷Flink入门到实战-学习笔记?id=滚动窗口tumbling-windows-1)

- 滚动窗口（Tumbling windows）要用Tumble类来定义

```java
// Tumbling Event-time Window（事件时间字段rowtime）
.window(Tumble.over("10.minutes").on("rowtime").as("w"))

// Tumbling Processing-time Window（处理时间字段proctime）
.window(Tumble.over("10.minutes").on("proctime").as("w"))

// Tumbling Row-count Window (类似于计数窗口，按处理时间排序，10行一组)
.window(Tumble.over("10.rows").on("proctime").as("w"))
```

- over：定义窗口长度
- on：用来分组（按时间间隔）或者排序（按行数）的时间字段
- as：别名，必须出现在后面的groupBy中



#### [滑动窗口(Sliding windows)](https://ashiamd.github.io/docsify-notes/#/study/BigData/Flink/尚硅谷Flink入门到实战-学习笔记?id=滑动窗口sliding-windows-1)

- 滑动窗口（Sliding windows）要用Slide类来定义

```java
// Sliding Event-time Window
.window(Slide.over("10.minutes").every("5.minutes").on("rowtime").as("w"))

// Sliding Processing-time window 
.window(Slide.over("10.minutes").every("5.minutes").on("proctime").as("w"))

// Sliding Row-count window
.window(Slide.over("10.rows").every("5.rows").on("proctime").as("w"))
```

- over：定义窗口长度
- every：定义滑动步长
- on：用来分组（按时间间隔）或者排序（按行数）的时间字段
- as：别名，必须出现在后面的groupBy中



#### [会话窗口(Session windows)](https://ashiamd.github.io/docsify-notes/#/study/BigData/Flink/尚硅谷Flink入门到实战-学习笔记?id=会话窗口session-windows-1)

- 会话窗口（Session windows）要用Session类来定义

```java
// Session Event-time Window
.window(Session.withGap("10.minutes").on("rowtime").as("w"))

// Session Processing-time Window 
.window(Session.withGap("10.minutes").on("proctime").as("w"))
```

- withGap：会话时间间隔
- on：用来分组（按时间间隔）或者排序（按行数）的时间字段
- as：别名，必须出现在后面的groupBy中