DirectKafkaInputDStream 只在 driver 端接收数据，所以继承了 InputDStream，是没有 receivers 的

---

在结合 Spark Streaming 及 Kafka 的实时应用中，我们通常使用以下两个 API 来获取最初的 DStream（这里不关心这两个 API 的重载）:

```
KafkaUtils#createDirectStream
```

及

```
KafkaUtils#createStream
```

这两个 API 除了要传入的参数不同外，接收 kafka 数据的节点、拉取数据的时机也完全不同。本文将分别就两者进行详细分析。

##KafkaUtils#createStream
先来分析 ```createStream```，在该函数中，会新建一个 ```KafkaInputDStream```对象，```KafkaInputDStream```继承于 ```ReceiverInputDStream```。我们在文章[揭开Spark Streaming神秘面纱② - ReceiverTracker 与数据导入](http://www.jianshu.com/p/3195fb3c4191)分析过

1. 继承ReceiverInputDStream的类需要重载 getReceiver 函数以提供用于接收数据的 receiver
2. recever 会调度到某个 executor 上并启动，不间断的接收数据并将收到的数据交由 ReceiverSupervisor 存成 block 作为 RDD 输入数据

KafkaInputDStream当然也实现了getReceiver方法，如下：

```
  def getReceiver(): Receiver[(K, V)] = {
    if (!useReliableReceiver) {
      //< 不启用 WAL
      new KafkaReceiver[K, V, U, T](kafkaParams, topics, storageLevel)
    } else {
      //< 启用 WAL
      new ReliableKafkaReceiver[K, V, U, T](kafkaParams, topics, storageLevel)
    }
  }
```

根据是否启用 WAL，receiver 分为 KafkaReceiver 和 ReliableKafkaReceiver。[揭开Spark Streaming神秘面纱②-ReceiverTracker 与数据导入](http://www.jianshu.com/p/3195fb3c4191)一文中详细地介绍了
1. receiver 是如何被分发启动的
2. receiver 接受数据后数据的流转过程
并在 [揭开Spark Streaming神秘面纱③ - 动态生成 job](http://www.jianshu.com/p/ee845802921e) 一文中详细介绍了
1. receiver 接受的数据存储为 block 后，如何将 blocks 作为 RDD 的输入数据
2. 动态生成 job

以上两篇文章并没有具体介绍 receiver 是如何接收数据的，当然每个重载了 ReceiverInputDStream 的类的 receiver 接收数据方式都不相同。下图描述了 KafkaReceiver 接收数据的具体流程：


![](http://upload-images.jianshu.io/upload_images/204749-360390c136ebe260.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

##KafkaUtils#createDirectStream
在[揭开Spark Streaming神秘面纱③ - 动态生成 job](http://www.jianshu.com/p/ee845802921e)中，介绍了在生成每个 batch 的过程中，会去取这个 batch 对应的 RDD，若未生成该 RDD，则会取该 RDD 对应的 blocks 数据来生成 RDD，最终会调用到```DStream#compute(validTime: Time)```函数，在```KafkaUtils#createDirectStream```调用中，会新建```DirectKafkaInputDStream```，```DirectKafkaInputDStream#compute(validTime: Time)```会从 kafka 拉取数据并生成 RDD，流程如下：


![](http://upload-images.jianshu.io/upload_images/204749-9d7be6b6c04c700b.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

如上图所示，该函数主要做了以下三个事情：

1. 确定要接收的 partitions 的 offsetRange，以作为第2步创建的 RDD 的数据来源
2. 创建 RDD 并执行 count 操作，使 RDD 真实具有数据
3. 以 streamId、数据条数，offsetRanges 信息初始化 inputInfo 并添加到 JobScheduler 中

进一步看 KafkaRDD 的 getPartitions 实现：

```
  override def getPartitions: Array[Partition] = {
    offsetRanges.zipWithIndex.map { case (o, i) =>
        val (host, port) = leaders(TopicAndPartition(o.topic, o.partition))
        new KafkaRDDPartition(i, o.topic, o.partition, o.fromOffset, o.untilOffset, host, port)
    }.toArray
  }
```

从上面的代码可以很明显看到，KafkaRDD 的 partition 数据与 Kafka topic 的某个 partition 的 o.fromOffset 至 o.untilOffset 数据是相对应的，也就是说 KafkaRDD 的 partition 与 Kafka partition 是一一对应的

---

通过以上分析，我们可以对这两种方式的区别做一个总结：

1. createStream会使用 Receiver；而createDirectStream不会
2. createStream使用的 Receiver 会分发到某个 executor 上去启动并接受数据；而createDirectStream直接在 driver 上接收数据
3. createStream使用 Receiver 源源不断的接收数据并把数据交给 ReceiverSupervisor 处理最终存储为 blocks 作为 RDD 的输入，从 kafka 拉取数据与计算消费数据相互独立；而createDirectStream会在每个 batch 拉取数据并就地消费，到下个 batch 再次拉取消费，周而复始，从 kafka 拉取数据与计算消费数据是连续的，没有独立开
4. createStream中创建的KafkaInputDStream 每个 batch 所对应的 RDD 的 partition 不与 Kafka partition 一一对应；而createDirectStream中创建的 DirectKafkaInputDStream 每个 batch 所对应的 RDD 的 partition 与 Kafka partition 一一对应

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
