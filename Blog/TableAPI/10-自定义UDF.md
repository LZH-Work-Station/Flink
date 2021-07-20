# 自定义函数

## 1. 标量函数(1 -> 1)

- 用户定义的标量函数，可以将0、1或多个标量值，映射到新的标量值
- 为了定义标量函数，必须在 org.apache.flink.table.functions 中扩展基类Scalar Function，并实现（一个或多个）求值（eval）方法
- **标量函数的行为由求值方法决定，求值方法必须public公开声明并命名为 eval**

```java
public static class HashCode extends ScalarFunction {

  private int factor = 13;

  public HashCode(int factor) {
    this.factor = factor;
  }

  public int eval(String id) {
    return id.hashCode() * 13;
  }
}
```





```java
public class UdfTest1_ScalarFunction {
  public static void main(String[] args) throws Exception {
    // 创建执行环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // 并行度设置为1
    env.setParallelism(1);

    // 创建Table执行环境
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    // 1. 读取数据
    DataStream<String> inputStream = env.readTextFile("/tmp/Flink_Tutorial/src/main/resources/sensor.txt");

    // 2. 转换成POJO
    DataStream<SensorReading> dataStream = inputStream.map(line -> {
      String[] fields = line.split(",");
      return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
    });

    // 3. 将流转换为表
    Table sensorTable = tableEnv.fromDataStream(dataStream, "id,timestamp as ts,temperature");

    // 4. 自定义标量函数，实现求id的hash值
    HashCode hashCode = new HashCode(23);
    // 注册UDF, 自定义UDF
    tableEnv.registerFunction("hashCode", hashCode);

    // 4.1 table API
    Table resultTable = sensorTable.select("id, ts, hashCode(id)");

    // 4.2 SQL
    tableEnv.createTemporaryView("sensor", sensorTable);
    Table resultSqlTable = tableEnv.sqlQuery("select id, ts, hashCode(id) from sensor");

    // 打印输出
    tableEnv.toAppendStream(resultTable, Row.class).print();
    tableEnv.toAppendStream(resultSqlTable, Row.class).print();

    env.execute();
  }

  public static class HashCode extends ScalarFunction {

    private int factor = 13;

    public HashCode(int factor) {
      this.factor = factor;
    }

    public int eval(String id) {
      return id.hashCode() * 13;
    }
  }
}
```

## 2. 表函数（1-> 多）

- 用户定义的表函数，也可以将0、1或多个标量值作为输入参数；**与标量函数不同的是，它可以返回任意数量的行作为输出，而不是单个值**

- 为了定义一个表函数，必须扩展 org.apache.flink.table.functions 中的基类 TableFunction 并实现（一个或多个）求值方法

- **表函数的行为由其求值方法决定，求值方法必须是 public 的，并命名为 eval**

  ```java
  public static class Split extends TableFunction<Tuple2<String, Integer>> {
  
    // 定义属性，分隔符
    private String separator = ",";
  
    public Split(String separator) {
      this.separator = separator;
    }
  
    public void eval(String str) {
      for (String s : str.split(separator)) {
        collect(new Tuple2<>(s, s.length()));
      }
    }
  }
  ```

```java
public class UdfTest2_TableFunction {
  public static void main(String[] args) throws Exception {
    // 创建执行环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // 并行度设置为1
    env.setParallelism(1);

    // 创建Table执行环境
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    // 1. 读取数据
    DataStream<String> inputStream = env.readTextFile("/tmp/Flink_Tutorial/src/main/resources/sensor.txt");

    // 2. 转换成POJO
    DataStream<SensorReading> dataStream = inputStream.map(line -> {
      String[] fields = line.split(",");
      return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
    });

    // 3. 将流转换为表
    Table sensorTable = tableEnv.fromDataStream(dataStream, "id,timestamp as ts,temperature");

    // 4. 自定义表函数，实现将id拆分，并输出（word, length）
    Split split = new Split("_");
    // 需要在环境中注册UDF
    tableEnv.registerFunction("split", split);

    // 4.1 table API
    Table resultTable = sensorTable
        // joinLateral 是 
      .joinLateral("split(id) as (word, length)")
      .select("id, ts, word, length");

    // 4.2 SQL
    tableEnv.createTemporaryView("sensor", sensorTable);
    Table resultSqlTable = tableEnv.sqlQuery("select id, ts, word, length " +
                                             " from sensor, lateral table(split(id)) as splitid(word, length)");

    // 打印输出
    tableEnv.toAppendStream(resultTable, Row.class).print("result");
    tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");

    env.execute();
  }

  // 实现自定义 Table Function
  public static class Split extends TableFunction<Tuple2<String, Integer>> {

    // 定义属性，分隔符
    private String separator = ",";

    public Split(String separator) {
      this.separator = separator;
    }

    public void eval(String str) {
      for (String s : str.split(separator)) {
        collect(new Tuple2<>(s, s.length()));
      }
    }
  }
}
```

