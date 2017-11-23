# 【容错篇】WAL在Spark Streaming中的应用
WAL 即 write ahead log（预写日志），是在 1.2 版本中就添加的特性。作用就是，将数据通过日志的方式写到可靠的存储，比如 HDFS、s3，在 driver 或 worker failure 时可以从在可靠存储上的日志文件恢复数据。WAL 在 driver 端和 executor 端都有应用。我们分别来介绍。

##WAL在 driver 端的应用
###何时创建
用于写日志的对象 ```writeAheadLogOption: WriteAheadLog```
 在 StreamingContext 中的 JobScheduler 中的 ReceiverTracker 的 ReceivedBlockTracker 构造函数中被创建，ReceivedBlockTracker 用于管理已接收到的 blocks 信息。需要注意的是，这里只需要启用 checkpoint 就可以创建该 driver 端的 WAL 管理实例，而不需要将 ```spark.streaming.receiver.writeAheadLog.enable``` 设置为 ```true```。

参见：[揭开Spark Streaming神秘面纱② - ReceiverTracker 与数据导入
](http://www.jianshu.com/p/3195fb3c4191)
###写什么、何时写
**写什么**
首选需要明确的是，ReceivedBlockTracker 通过 WAL 写入 log 文件的内容是3种事件（当然，会进行序列化）：

* case class BlockAdditionEvent(receivedBlockInfo: ReceivedBlockInfo)；即新增了一个 block 及该 block 的具体信息，包括 streamId、blockId、数据条数等
* case class BatchAllocationEvent(time: Time, allocatedBlocks: AllocatedBlocks)；即为某个 batchTime 分配了哪些 blocks 作为该 batch RDD 的数据源
* case class BatchCleanupEvent(times: Seq[Time])；即清理了哪些 batchTime 对应的 blocks

知道了写了什么内容，结合源码，也不难找出是什么时候写了这些内容。需要再次注意的是，写上面这三种事件，也不需要将 ```spark.streaming.receiver.writeAheadLog.enable``` 设置为 ```true```。

**何时写BlockAdditionEvent**
在[揭开Spark Streaming神秘面纱② - ReceiverTracker 与数据导入
](http://www.jianshu.com/p/3195fb3c4191)一文中，已经介绍过当 Receiver 接收到数据后会调用 ```ReceiverSupervisor#pushAndReportBlock```方法，该方法将 block 数据存储并写一份到日志文件中（即 WAL），之后最终将 block 信息，即 ```receivedBlockInfo```（包括 streamId、batchId、数据条数）传递给 ```ReceivedBlockTracker```.

当 ```ReceivedBlockTracker``` 接收到 ```receivedBlockInfo``` 后，将之封装成 ```BlockAdditionEvent(receivedBlockInfo)``` 并写入日志（WAL）。

抛开代码调用逻辑不谈，一句话总结的话，就是当 Receiver 接收数据产生新的 block 时，最终会触发产生并写 ```BlockAdditionEvent```

**何时写BatchAllocationEvent**
在[揭开Spark Streaming神秘面纱③ - 动态生成 job](http://www.jianshu.com/p/ee845802921e)一文中介绍了 JobGenerator 每隔 batch duration 就会为这个 batch 生成对应的 jobs。在生成 jobs 的时候需要为 RDD 提供数据，这个时候就会触发执行

```
jobScheduler.receiverTracker.allocateBlocksToBatch(time)
```

该操作将把所有该 streamId 对应的已接收存储但未分配的 blocks 都分配给该 batch，我们知道，ReceivedBlockTracker 保存着所有的 blocks 信息，所以为某个 batch 分配 blocks 这个分配请求最终会给到 ReceivedBlockTracker，ReceivedBlockTracker 在确认要分配哪些 blocks 之后，会将给某个 batchTime 分配了哪些 blocks 的对应关系封装成 ```BatchAllocationEvent(batchTime, allocatedBlocks)``` 并写入日志文件（WAL），这之后才进行真正的分配。

**何时写BatchCleanupEvent**

从我以前写的一些文章中可以知道，一个 batch 对应的是一个 jobSet，因为在一个 batch 可能会有多个 DStream 执行了多次 output 操作，每个 output 操作都将生成一个 job，这些 job 将组成 jobSet。总共有两种时机会触发将 ```BatchCleanupEvent``` 事件写入日志（WAL），我们进行依次介绍

我们先来介绍第一种，废话不多说，直接看具体步骤：

1. 每当 jobSet 中某一个 job 完成的时候，job 调度器会去检查该 job 对应的 jobSet 中的所有 job 是否均已完成
2. 若是，会通过 jobGenerator.eventLoop 给自身发送 ```ClearMetadata``` 消息
3. jobGenerator 在接收到该消息后，调用自身clearMetadata方法，clearMetadata方法最终会调用到 ```ReceiverTracker#cleanupOldBlocksAndBatches```，具体cleanupOldBlocksAndBatches方法干了什么稍后分析

另一种时机如下：

1. JobGenerator在完成 checkpoint 时，会给自身发送一个 ```ClearCheckpointData``` 消息
2. JobGenerator在收到 ```ClearCheckpointData``` 消息后，调用 ```clearCheckpointData``` 方法
3. 在 ```JobGenerator#ClearCheckpointData``` 方法中，会调用到 ```ReceiverTracker#drcleanupOldBlocksAndBatches```

从上面的两小段分析我们可以知道，当一个 batch 的 jobSet 中的 jobs 都完成的时候和每次 checkpoint操作完成的时候会触发执行 ```ReceiverTracker#cleanupOldBlocksAndBatches``` 方法，该方法里做了什么呢？见下图：
![](http://upload-images.jianshu.io/upload_images/204749-71627efceab321ae.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


上图描述了以上两个时机下，是如何：

1. 将 batch cleanup 事件写入 WAL 中
2. 清理过期的 blocks 及 batches 的元数据
3. 清理过期的 blocks 数据（只有当将 ```spark.streaming.receiver.writeAheadLog.enable``` 设置为 ```true```才会执行这一步）

##WAL 在 executor 端的应用
Receiver 接收到的数据会源源不断的传递给 ReceiverSupervisor，是否启用 WAL 机制（即是否将 ```spark.streaming.receiver.writeAheadLog.enable``` 设置为 ```true```）会影响 ReceiverSupervisor 在存储 block 时的行为：

* 不启用 WAL：你设置的StorageLevel是什么，就怎么存储。比如MEMORY_ONLY只会在内存中存一份，MEMORY_AND_DISK会在内存和磁盘上各存一份等
* 启用 WAL：在StorageLevel指定的存储的基础上，写一份到 WAL 中。存储一份在 WAL 上，更不容易丢数据但性能损失也比较大

关于什么时候以及如何清理存储在 WAL 中的过期的数据已在上图中说明

##WAL 使用建议
关于是否要启用 WAL，要视具体的业务而定：

* 若可以接受一定的数据丢失，则不需要启用 WAL，因为对性能影响较大
* 若完全不能接受数据丢失，那就需要同时启用 checkpoint 和 WAL，checkpoint 保存着执行进度（比如已生成但未完成的 jobs），WAL 中保存着 blocks 及 blocks 元数据（比如保存着未完成的 jobs 对应的 blocks 信息及 block 文件）。同时，这种情况可能要在数据源和 Streaming Application 中联合来保证 exactly once 语义

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
