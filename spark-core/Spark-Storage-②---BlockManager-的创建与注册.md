> 本文为 Spark 2.0 源码分析笔记，某些实现可能与其他版本有所出入

[上一篇文章](http://www.jianshu.com/p/730eed6a98d2)介绍了 Spark Storage 模块的整体架构，本文将着手介绍在 Storeage Master 和 Slave 上发挥重要作用的 BlockManager 是在什么时机以及如何创建以及注册的。接下来分别介绍 Master 端和 Slave 端的 BlockManager。

为了方便阅读，后文中将以 Master 作为 Storage Master（driver） 端的 BlockManager 的简称，以 Slave 作为 Storage Slave（executor） 端的 BlockManager 的简称。

## BlockManager 创建时机
### Master 创建时机
在 driver 端，构造 SparkContext 时会创建 SparkEnv 实例 _env，创建 _env 是通过调用 object SparkEnv 的 create 方法，在该方法中会创建 Master，即 driver 端的 blockManager。

所以，简单来说，Master 是在 driver 创建 SparkContext 时就创建了。

### Slave 创建时机
在 worker 进程起来的的时候，```object CoarseGrainedExecutorBackend``` 初始化时会通过调用 ```SparkEnv#createExecutorEnv```，在该函数中会创建 executor 端的 BlockManager，也即 Slave。这之后，CoarseGrainedExecutorBackend 才向 driver 注册 executor，然后再构造 Executor 实例。

接下来，我们看看 BlockManager 是如何创建的。

## 创建 BlockManager
一图胜千言，我们还是先来看看 Master 是如何创建的：


![图1: 创建 BlockManage](http://upload-images.jianshu.io/upload_images/204749-fc5dbb6906d8b93e.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)



结合上图我们来进行 Step By Step 的分析

### Step1: 创建 RpcEnv 实例 rpcEnv
这一步通过 systemName、hostname、port 等创建一个 RpcEnv 类型实例 rpcEnv，更具体的说是一个 NettRpcEnv 实例，在 Spark 2.0 中已经没有 akka rpc 的实现，该 rpcEnv 实例用于：

* 接受稍后创建的 rpcEndpoint 的注册并持有 rpcEndpoint（该 rpcEndpoint 用于接收对应的 rpcEndpointRef 发送的消息以及将消息指派给相应的函数处理）
* 持有一个消息分发器 ```dispatcher: Dispatcher```，将接收到的消息分发给相应的 rpcEndpoint 处理

### Step2: 创建 BlockManagerMaster 实例 blockManagerMaster
BlockManagerMaster 持有 driverRpcEndpointRef，其包含各种方法通过该 driverRpcEndpointRef 来给 Master 发送各种消息来实现注册 BlockManager、移除 block、获取/更新 block、移除 Broadcast 等功能。

如上图所示，创建 BlockManagerMaster 的流程如下：

1. 先创建 BlockManagerMasterEndpoint 实例
2. 对于  master（on driver），将上一步得到的 blockManagerMasterEndpoint 注册到 driverRpcEnv，以供之后driverRpcEnv 中的消息分发器分发消息给它来处理特定的消息，并返回 driverRpcEndpointRef；而对于 slave（on executor），通过 driverHost、driverPort 获取 driverRpcEndpointRef
3. 利用上一步构造的 driverRpcEndpointRef，结合 sparkConf 及是否是 driver 标记来构造 BlockManagerMaster 实例

### Step3: 创建 BlockManager 实例
结合 Step1 中创建的 rpcEnv，Step2 中创建的 blockManagerMaster 以及 executorId、memoryManager、mapOutputTracker、shuffleManager 等创建 BlockManager 实例。该 BlockManager 也就是 Storage 模块的 Master 或 Slave 了。

BlockManager 运行在所有的节点上，包括 driver 和 executor，用来存取在本地或远程节点上的 blocks，blocks 可以是在内存中、磁盘上火对外内存中。

## 注册 BlockManager
BlockManager 实例在被创建后，不能直接使用，必须调用其 ```initialize``` 方法才能使用。对于 Master，是在 BlockManager 创建后就调用了 ```initialize``` 方法；对于 Slave，是在 Executor 的构造函数中调用 ```initialize``` 方法进行初始化。

在 ```initialize``` 方法中，会进行 BlockManager 的注册，具体操作时通过 driverRpcEndpointRef 发送 ```RegisterBlockManager``` 消息

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
