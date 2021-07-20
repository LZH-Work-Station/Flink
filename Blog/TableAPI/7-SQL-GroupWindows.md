# SQL groupWindows

Group Windows定义在SQL查询的Group By子句中

- TUMBLE(time_attr, interval)
  - 定义一个滚动窗口，每一个参数是时间字段，第二个参数是窗口长度
- HOP(time_attr，interval，interval)
  - 定义一个滑动窗口，第一个参数是时间字段，**第二个参数是窗口滑动步长，第三个是窗口长度**
- SESSION(time_attr，interval)
  - 定义一个绘画窗口，第一个参数是时间字段，第二个参数是窗口间隔

#### [测试代码](https://ashiamd.github.io/docsify-notes/#/study/BigData/Flink/尚硅谷Flink入门到实战-学习笔记?id=测试代码-8)

```java
package apitest.tableapi;

import apitest.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author : Ashiamd email: ashiamd@foxmail.com
 * @date : 2021/2/4 12:47 AM
 */
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
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
        @Override
        public long extractTimestamp(SensorReading element) {
          return element.getTimestamp() * 1000L;
        }
      });

    // 4. 将流转换成表，定义时间特性
    Table dataTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp, rt.rowtime");

    tableEnv.createTemporaryView("sensor", dataTable);

    // 5. 窗口操作
    // 5.1 Group Window
    // table API
      // rt是我们的 rowtime，指我们根据event time 来开窗
    Table resultTable = dataTable.window(Tumble.over("10.seconds").on("rt").as("tw"))
      .groupBy("id, tw")
        // 将同一个窗口的 相同id作聚合，计算数量和平均值
      .select("id, id.count, temp.avg, tw.end");

    // SQL
      // 跟上面同一个意思
    Table resultSqlTable = tableEnv.sqlQuery(
   "select id, count(id) as cnt, avg(temp) as avgTemp, tumble_end(rt, interval '10' second) " +
   "from sensor group by id, tumble(rt, interval '10' second)");

    dataTable.printSchema();
    tableEnv.toAppendStream(resultTable, Row.class).print("result");
    tableEnv.toRetractStream(resultSqlTable, Row.class).print("sql");

    env.execute();
  }
}
```

输出：

```java
root
 |-- id: STRING
 |-- ts: BIGINT
 |-- temp: DOUBLE
 |-- rt: TIMESTAMP(3) *ROWTIME*

result> sensor_1,1,35.8,2019-01-17T09:43:20
result> sensor_6,1,15.4,2019-01-17T09:43:30
result> sensor_1,2,34.55,2019-01-17T09:43:30
result> sensor_10,1,38.1,2019-01-17T09:43:30
result> sensor_7,1,6.7,2019-01-17T09:43:30
sql> (true,sensor_1,1,35.8,2019-01-17T09:43:20)
result> sensor_1,1,37.1,2019-01-17T09:43:40
sql> (true,sensor_6,1,15.4,2019-01-17T09:43:30)
sql> (true,sensor_1,2,34.55,2019-01-17T09:43:30)
sql> (true,sensor_10,1,38.1,2019-01-17T09:43:30)
sql> (true,sensor_7,1,6.7,2019-01-17T09:43:30)
sql> (true,sensor_1,1,37.1,2019-01-17T09:43:40)
```