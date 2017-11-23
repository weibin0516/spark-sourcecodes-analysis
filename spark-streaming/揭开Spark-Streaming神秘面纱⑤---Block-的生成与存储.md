ReceiverSupervisorImpl共提供了4个将从 receiver 传递过来的数据转换成 block 并存储的方法，分别是：

* pushSingle: 处理单条数据
* pushArrayBuffer: 处理数组形式数据
* pushIterator: 处理 iterator 形式处理
* pushBytes: 处理 ByteBuffer 形式数据

其中，pushArrayBuffer、pushIterator、pushBytes最终调用pushAndReportBlock；而pushSingle将调用defaultBlockGenerator.addData(data)，我们分别就这两种形式做说明

##pushAndReportBlock
我们针对存储 block 简化 pushAndReportBlock 后的代码如下：

```
def pushAndReportBlock(
  receivedBlock: ReceivedBlock,
  metadataOption: Option[Any],
  blockIdOption: Option[StreamBlockId]
) {
  ...
  val blockId = blockIdOption.getOrElse(nextBlockId)
  receivedBlockHandler.storeBlock(blockId, receivedBlock)
  ...
}
```

首先获取一个新的 blockId，之后调用 ```receivedBlockHandler.storeBlock```, ```receivedBlockHandler``` 在 ```ReceiverSupervisorImpl``` 构造函数中初始化。当启用了 checkpoint 且 ```spark.streaming.receiver.writeAheadLog.enable``` 为 ```true``` 时，```receivedBlockHandler``` 被初始化为 ```WriteAheadLogBasedBlockHandler``` 类型；否则将初始化为 ```BlockManagerBasedBlockHandler```类型。

```WriteAheadLogBasedBlockHandler#storeBlock``` 将 ArrayBuffer, iterator, bytes 类型的数据序列化后得到的 serializedBlock

1. 交由 BlockManager 根据设置的 StorageLevel 存入 executor 的内存或磁盘中
2. 通过 WAL 再存储一份

而```BlockManagerBasedBlockHandler#storeBlock```将 ArrayBuffer, iterator, bytes 类型的数据交由 BlockManager 根据设置的 StorageLevel 存入 executor 的内存或磁盘中，并不再通过 WAL 存储一份

##pushSingle
pushSingle将调用 BlockGenerator#addData(data: Any) 通过积攒的方式来存储数据。接下来对 BlockGenerator 是如何积攒一条一条数据最后写入 block 的逻辑。


![](http://upload-images.jianshu.io/upload_images/204749-7db72fcc95767ec3.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

上图为 BlockGenerator 的各个成员，首选对各个成员做介绍：

###currentBuffer
变长数组，当 receiver 接收的一条一条的数据将会添加到该变长数组的尾部

* 可能会有一个 receiver 的多个线程同时进行添加数据，这里是同步操作
* 添加前，会由 rateLimiter 检查一下速率，是否加入的速度过快。如果过快的话就需要 block 住，等到下一秒再开始添加。最高频率由 ```spark.streaming.receiver.maxRate``` 控制，默认值为 ```Long.MaxValue```，具体含义是单个 Receiver 每秒钟允许添加的条数。

###blockIntervalTimer & blockIntervalMs
分别是定时器和时间间隔。blockIntervalTimer中有一个线程，每隔blockIntervalMs会执行以下操作：

1. 将 currentBuffer 赋值给 newBlockBuffer
2. 将 currentBuffer 指向新的空的 ArrayBuffer 对象
3. 将 newBlockBuffer 封装成 newBlock
4. 将 newBlock 添加到 blocksForPushing 队列中

blockIntervalMs 由 ```spark.streaming.blockInterval``` 控制，默认是 200ms。

###blockPushingThread & blocksForPushing & blockQueueSize
blocksForPushing 是一个定长数组，长度由 blockQueueSize 决定，默认为10，可通过 ```spark.streaming.blockQueueSize``` 改变。上面分析到，blockIntervalTimer中的线程会定时将 block 塞入该队列。

还有另一条线程不断送该队列中取出 block，然后调用 ```ReceiverSupervisorImpl.pushArrayBuffer(...)``` 来将 block 存储，这条线程就是blockPushingThread。

PS: blocksForPushing为ArrayBlockingQueue类型。ArrayBlockingQueue是一个阻塞队列，能够自定义队列大小，当插入时，如果队列已经没有空闲位置，那么新的插入线程将阻塞到该队列，一旦该队列有空闲位置，那么阻塞的线程将执行插入


以上，通过分析各个成员，也说明了 BlockGenerator 是如何存储单条数据的。

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
