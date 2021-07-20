# Over开窗函数

- **Over window 聚合是标准 SQL 中已有的（over 子句），可以在查询的 SELECT 子句中定义**

- Over window 聚合，会**针对每个输入行**，计算相邻行范围内的聚合

- Over windows 使用 window（w:overwindows*）子句定义，并在 select（）方法中通过**别名**来引用

  ```scala
  Table table = input
  .window([w: OverWindow] as "w")
  // 使用over进行 窗口内聚合运算
  .select("a, b.sum over w, c.min over w");
  ```

- Table API 提供了 Over 类，来配置 Over 窗口的属性

#### [无界Over Windows](https://ashiamd.github.io/docsify-notes/#/study/BigData/Flink/尚硅谷Flink入门到实战-学习笔记?id=无界over-windows)

- 可以在事件时间或处理时间，以及指定为时间间隔、或行计数的范围内，定义 Over windows
- 无界的 over window 是使用常量指定的
- partitionBy是可选项

```java
// 无界的事件时间over window (时间字段 "rowtime")
.window(Over.partitionBy("a").orderBy("rowtime").preceding(UNBOUNDED_RANGE).as("w"))

//无界的处理时间over window (时间字段"proctime")
.window(Over.partitionBy("a").orderBy("proctime").preceding(UNBOUNDED_RANGE).as("w"))

// 无界的事件时间Row-count over window (时间字段 "rowtime")
.window(Over.partitionBy("a").orderBy("rowtime").preceding(UNBOUNDED_ROW).as("w"))

//无界的处理时间Row-count over window (时间字段 "rowtime")
.window(Over.partitionBy("a").orderBy("proctime").preceding(UNBOUNDED_ROW)
```



#### [有界Over Windows](https://ashiamd.github.io/docsify-notes/#/study/BigData/Flink/尚硅谷Flink入门到实战-学习笔记?id=有界over-windows)

- 有界的over window是用间隔的大小指定的
- partitionBy是可选项

```java
// 有界的事件时间over window (时间字段 "rowtime"，之前1分钟)
.window(Over.partitionBy("a").orderBy("rowtime").preceding("1.minutes").as("w"))

// 有界的处理时间over window (时间字段 "rowtime"，之前1分钟)
.window(Over.partitionBy("a").orderBy("porctime").preceding("1.minutes").as("w"))

// 有界的事件时间Row-count over window (时间字段 "rowtime"，之前10行)
.window(Over.partitionBy("a").orderBy("rowtime").preceding("10.rows").as("w"))

// 有界的处理时间Row-count over window (时间字段 "rowtime"，之前10行)
.window(Over.partitionBy("a").orderBy("proctime").preceding("10.rows").as
```

### [12.4.4 SQL中的Over Windows](https://ashiamd.github.io/docsify-notes/#/study/BigData/Flink/尚硅谷Flink入门到实战-学习笔记?id=_1244-sql中的over-windows)

- 用 Over 做窗口聚合时，所有聚合必须在同一窗口上定义，也就是说必须是相同的分区、排序和范围
- 目前仅支持在当前行范围之前的窗口
- ORDER BY 必须在单一的时间属性上指定

```sql
SELECT COUNT(amount) OVER (
  PARTITION BY user
  ORDER BY proctime
  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
FROM Orders

// 也可以做多个聚合
SELECT COUNT(amount) OVER w, SUM(amount) OVER w
FROM Orders
WINDOW w AS (
  PARTITION BY user
  ORDER BY proctime
  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
```