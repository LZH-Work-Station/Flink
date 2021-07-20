# Checkpoints的描述

### 1. 定义

CheckPoint是某个时间的一个快照，这个时间必须是恰好处理完一个相同的输入数据。例如我们传入1， 2， 3， 4， 5。当传到3的时候，只有我们确认了，3已经被所有operator处理完的时候我们才会对该时间进行一个拷贝。

- **Flink的操作**

Flink的checkPoint是分布式保存，每个分支保存自己的快照，然后把大家的快照P在一起

1. Flink 的检查点算法用到了一种称为分界线（barrier）的特殊数据形式，用来把一条流上数据按照不同的检查点分开
2. 分界线之前到来的数据导致的状态更改，都会被包含在当前分界线所属的检查点中；而基于分界线之后的数据导致的所有更改，就会被包含在之后的检查点中

### 具体过程

![image-20210716162324085](D:\Tutorial\Flink\src\main\java\CheckPoint\image\image-20210716162324085.png)

- JobManager 在需要创建checkPoint的时候 告诉所有数据源source我要开始创建checkPoint了，我的checkPoint的id是2

![image-20210716162552721](D:\Tutorial\Flink\src\main\java\CheckPoint\image\image-20210716162552721.png)

1. Source接收到checkpoint发出的barrier，将自己现在数据的偏移量记录，并返回一个ack给jobManager，告诉他ckeckpoin（id2）已经完成，并且告诉jobManager我存放的偏移量的地址。然后source通过广播发给 下游的所有operator。
2. 下游的operator等待上游 的所有source的barrier(**图中的三角**)请求到，才会开始 进行checkpoint的保存

![image-20210716163831044](D:\Tutorial\Flink\src\main\java\CheckPoint\image\image-20210716163831044.png)

1. 由于我们的source保存的checkpoint是(3, 4)，然后source在发送下一条数据之前，会将barrier请求发给下游，所以对于下游需要记录的是 算到（3， 4）的数据。
2. 如果由于下游等待barrier的时间过久，就会导致某一个source的下一个数据都到了，另一个source的barrier请求还没到。下游就会对到来的数据做一个缓存，先不做具体的计算，等到另一个barrier到达，然后写入checkpoint给jobManager之后再从缓存中读取数据然后进行计算

