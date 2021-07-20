# SQL 创建表

```java
String sinkDDL = 
  "create table dataTable (" +
  " id varchar(20) not null, " +
  " ts bigint, " +
  " temperature double, " +
  " pt AS PROCTIME() " +
  " ) with (" +
  " 'connector.type' = 'filesystem', " +
  " 'connector.path' = '/sensor.txt', " +
  " 'format.type' = 'csv')";

tableEnv.sqlUpdate(sinkDDL);
```

测试从文件中读取信息转化成表

- 在创建表的时候.proctim方法可以获得Process Time
- 在SQL中使用timestamp.rowtime可以把秒的数据转化成时间戳（可能需要前面定义水位线才可以）
- rt.rowtime 追加时间戳，追加时间戳需要前面定义水位线才可以。

```java
public class TableTest5_TimeAndWindow {
  public static void main(String[] args) throws Exception {
    // 1. 创建环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    // 2. 读入文件数据，得到DataStream
    DataStream<String> inputStream = env.readTextFile("/tmp/Flink_Tutorial/src/main/resources/sensor.txt");

    // 3. 转换成POJO
    DataStream<SensorReading> dataStream = inputStream.map(line -> {
      String[] fields = line.split(",");
      return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
    });

    // 4. 将流转换成表，定义时间特性
    Table dataTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp, pt.proctime");
      // 将BigInt类型的time转换成时间戳
    Table dataTable = tableEnv.fromDataStream(dataStream, "id, timestamp.rowtime as ts, temperature as temp");
      // 追加一个事件时间戳
    Table dataTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp, rt.rowtime");

    dataTable.printSchema();
    tableEnv.toAppendStream(dataTable, Row.class).print();

    env.execute();
  }
}
```

## Table API 的事件时间定义

![image-20210719230032990](C:\Users\LI ZEHAN\AppData\Roaming\Typora\typora-user-images\image-20210719230032990.png)

### DDL 事件时间定义

![image-20210719230100767](C:\Users\LI ZEHAN\AppData\Roaming\Typora\typora-user-images\image-20210719230100767.png)
