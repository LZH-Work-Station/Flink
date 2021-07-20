# Over和Group的区别

GroupWindow: 可以是各种滚动窗口滑动窗口等，而且窗口作为一个变量，可以被groupBy或者orderBy等等

OverWindow: 只是依据窗口间隔做聚合

```java
// 5. 窗口操作
    // 5.1 Group Window
    // table API
    Table resultTable = dataTable.window(Tumble.over("10.seconds").on("rt").as("tw"))
      .groupBy("id, tw")
      .select("id, id.count, temp.avg, tw.end");

    // SQL
    Table resultSqlTable = tableEnv.sqlQuery(
        "select id, count(id) as cnt, avg(temp) as avgTemp, tumble_end(rt, interval '10' second) " +
        "from sensor group by id, tumble(rt, interval '10' second)");

    // 5.2 Over Window
    // table API
    Table overResult = dataTable.window(Over.partitionBy("id").orderBy("rt").preceding("2.rows").as("ow"))
      .select("id, rt, id.count over ow, temp.avg over ow");

    // SQL
    Table overSqlResult = tableEnv.sqlQuery(
        " select id, rt, count(id) over ow, avg(temp) over ow " +
        " from sensor " +
        " window ow as (partition by id order by rt rows between 2 preceding and current row)");


    tableEnv.toAppendStream(overResult, Row.class).print("result");
    tableEnv.toRetractStream(overSqlResult, Row.class).print("sql");
```

