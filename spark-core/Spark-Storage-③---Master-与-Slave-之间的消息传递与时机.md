> 本文为 Spark 2.0 源码分析笔记，某些实现可能与其他版本有所出入

再次重申标题中的 Master 是指 Spark Storage 模块的 Master，是运行在 driver 上的 BlockManager 及其包含的 BlockManagerMaster、RpcEnv 及 RpcEndpoint 等；而 Slave 则是指 Spark Storage 模块的 Slave，是运行在 executor 上的 BlockManager 及其包含的 BlockManagerMaster、RpcEnv 及 RpcEndpoint 等。下文也将沿用 Master 和 Slave 简称。

Master 与 Slaves 之间是通过消息进行通信的，本文将分析 Master 与 Slaves 之间重要的消息以及这些消息是在什么时机被触发发送的。

## Master -> Slave
先来看看 Master 都会发哪些消息给 Slave

### case class RemoveBlock(blockId: BlockId)
用于移除 slave 上的 block。在以下两个时机会触发：

* task 结束时
* Spark Streaming 中，清理过期的 batch 对应的 blocks

---

### case class RemoveRdd(rddId: Int)
用于移除归属于某个 RDD 的所有 blocks，触发时机：

* 释放缓存的 RDD

---

### case class RemoveShuffle(shuffleId: Int)
用于移除归属于某次 shuffle 所有的 blocks，触发时机：

* 做 shuffle 清理的时候

---

### case class RemoveBroadcast(broadcastId: Long, removeFromDriver: Boolean = true)
用于移除归属于特定 Broadcast 的所有 blocks。触发时机：

* 调用 ```Broadcast#destroy``` 销毁广播变量
* 调用 ```Broadcast#unpersist``` 删除 executors 上的广播变量拷贝

接下来看看 Slaves 发送给 Master 的消息

## Slave -> Master
### case class RegisterBlockManager(blockManagerId: BlockManagerId ...)
用于 Slave（executor 端 BlockManager） 向 Master（driver 端 BlockManager） 注册，触发时机：

* executor 端 BlockManager 在初始化时

---

### case class UpdateBlockInfo(var blockManagerId: BlockManagerId, var blockId: BlockId ...)
用于向 Master 汇报指定 block 的信息，包括：storageLevel、存储在内存中的 size、存储在磁盘上的 size、是否 cached 等。触发时机：

* BlockManager 注册时
* block 被移除时
* 原本存储在内存中的 block 因内存不足而转移到磁盘上时
* 生成新的 block 时

---

### case class GetLocations(blockId: BlockId)
用于获取指定 blockId 的 block 所在的 BlockManagerId 列表，触发时机：

* 检查是否包含某个 block
* 以序列化形式读取本地或远程 BlockManagers 上的数据时
    * 读取以 blocks 形式存储的 task result 时
    * 读取 Broadcast blocks 数据时
    * 获取指定 block id 对应的 block 数据（比如获取 RDD partition 对应的 block）

---

### case class RemoveExecutor(execId: String)
用于移除已 lost 的 executor 上的 BlockManager（只在 driver 端进行操作），触发时机：

* executor lost（一般由于 task 连续失败导致）

---

### case object StopBlockManagerMaster
用于停止 driver 或 executor 端的 BlockManager，触发时机：

* SparkContext#stop 被调用时，也即 driver 停止时

---

### case object GetMemoryStatus
用于获取各个 BlockManager 的内存使用情况，包括最大可用内存以及当前可用内存（当前可用内存=最大可用内存-已用内存）

---

### case object GetStorageStatus
用于获取各个 BlockManager 的存储状态，包括每个 BlockManager 中都存储了哪些 RDD 的哪些 block（对应 partition）以及各个 block 的信息

---

### case class BlockManagerHeartbeat(blockManagerId: BlockManagerId)
用于 Slave 向 Master 发心跳信息，以通知 Master 其上的某个 BlockManager 还存活着

---

### case class HasCachedBlocks(executorId: String)
用于检查 executor 是否有缓存 blocks（广播变量的 blocks 不作考虑，因为广播变量的 block 不会汇报给 Master），触发时机：

* 检验某个 executor 是否闲置了一段时间，即一段时间内没有运行任何 tasks（这样的 executor 会慢慢被移除）

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
