# Table API 

## 1. 导入依赖

```xml
<dependency>
 <groupId>org.apache.flink</groupId>
 <artifactId>flink-table-planner_2.12</artifactId>
 <version>1.10.1</version>
</dependency>
<dependency>
 <groupId>org.apache.flink</groupId>
 <artifactId>flink-table-api-scala-bridge_2.12</artifactId>
 <version>1.10.1</version>
</dependency>
```

## 2. 基本程序结构



![image-20210717175323431](D:\Tutorial\Flink\Blog\TableAPI\image\image-20210717175323431.png)

### 1.创建表的基本方式

- 通过flink流创建表

```java
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
```

- 调用connect方法链接外部系统，并通过 createTemporaryTable()在catalog中注册

![image-20210717180118778](D:\Tutorial\Flink\Blog\TableAPI\image\image-20210717180118778.png)

**实例**：

![image-20210717180156920](D:\Tutorial\Flink\Blog\TableAPI\image\image-20210717180156920.png)

