> 本文为 Spark 2.0 源码分析笔记，某些实现可能与其他版本有所出入

Storage 模块在整个 Spark 中扮演着重要的角色，管理着 Spark Application 在运行过程中产生的各种数据，包括基于磁盘和内存的，比如 RDD 缓存，shuffle 过程中缓存及写入磁盘的数据，广播变量等。

Storage 模块也是 Master/Slave 架构，Master 是运行在 driver 上的 BlockManager实例，Slave 是运行在 executor 上的 BlockManager 实例。

Master 负责：

* 接受各个 Slaves 注册
* 保存整个 application 各个 blocks 的元数据
* 给各个 Slaves 下发命令

Slave 负责：

* 管理存储在其对应节点内存、磁盘上的 Blocks 数据
* 接收并执行 Master 的命令
* 更新 block 信息给 Master

整体架构图如下（包含1个 Master 和4个 Slaves）：


![Storage 模块 Master Slaves 架构.jpg](http://upload-images.jianshu.io/upload_images/204749-02771855cda616cb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)



在 driver 端，创建 SparkContext 时会创建 driver 端的 SparkEnv，在构造 SparkEnv 时会创建 BlockManager，而该 BlockManager 持有 RpcEnv 和 BlockManagerMaster。其中，RpcEnv 包含 driverRpcEndpoint 和各个 Slave 的 rpcEndpointRef（存储在```blockManagerInfo: mutable.HashMap[BlockManagerId, BlockManagerInfo]``` 中，BlockManagerInfo 包含对应 Slave 的 rpcEndpointRef），Storage Master 就是通过这些 Slaves 的 rpcEndpointRef 来给 Storage Slave 发送消息下达命令的

而在 slave 端（各个 executor），同样会创建 SparkEnv，创建 SparkEnv 时同样会创建 BlockManager，slave 端的 BlockManager 同样会持有 RpcEnv 以及 BlockManagerMaster。不同的是，slave 端的 RpcEnv 包含了 slaveRpcEndpoint 而 BlockManagerMaster 持有 driverRpcEndpoint， Storage Slave 就是通过 driverRpcEndpoint 来给 Storage Master 发送消息的

好，基于上图和相应的文字说明相信能对 Spark Storage 模块的整体架构有个大致的了解，更深入的分析将在之后的文章中进行~

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
