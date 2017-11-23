本文简述如何结合 Spark Streaming 和 Kakfa 来做实时计算。截止目前（2016-03-27）有两种方式：

1. 使用 kafka high-level API 和 Receivers，不需要自己管理 offsets
2. 不使用 Receivers 而直接拉取 kafka 数据，需要自行管理 offsets

两种方式在编程模型、运行特性、语义保障方面均不相同，让我们进一步说明。
如果你对 Receivers 没有概念，请先移步：[揭开Spark Streaming神秘面纱② - ReceiverTracker 与数据导入](http://www.jianshu.com/p/3195fb3c4191)

##方式一：Receiver-based
这种方法使用一个 Receiver 来接收数据。在该 Receiver 的实现中使用了 Kafka high-level consumer API。Receiver 从 kafka 接收的数据将被存储到 Spark executor 中，随后启动的 job 将处理这些数据。

在默认配置下，该方法失败后会丢失数据（保存在 executor 内存里的数据在 application 失败后就没了），若要保证数据不丢失，需要启用 WAL（即预写日志至 HDFS、S3等），这样再失败后可以从日志文件中恢复数据。WAL 相关内容请参见：http://spark.apache.org/docs/latest/streaming-programming-guide.html#deploying-applications

---

接下来讨论如何在 streaming application 中应用这种方法。使用 ```KafkaUtils.createStream```，实例代码如下：

```
def main(args: Array[String]) {
  if (args.length < 4) {
    System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
    System.exit(1)
  }

  StreamingExamples.setStreamingLogLevels()

  val Array(zkQuorum, group, topics, numThreads) = args
  val sparkConf = new SparkConf().setAppName("KafkaWordCount")
  val ssc = new StreamingContext(sparkConf, Seconds(2))
  ssc.checkpoint("checkpoint")

  val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
  val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
  val words = lines.flatMap(_.split(" "))
  val wordCounts = words.map(x => (x, 1L))
    .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
  wordCounts.print()

  ssc.start()
  ssc.awaitTermination()
}
```

需要注意的点：

* Kafka Topic 的 partitions 与RDD 的 partitions 没有直接关系，不能一一对应。如果增加 topic 的 partition 个数的话仅仅会增加单个 Receiver 接收数据的线程数。事实上，使用这种方法只会在一个 executor 上启用一个 Receiver，该 Receiver 包含一个线程池，线程池的线程个数与所有 topics 的 partitions 个数总和一致，每条线程接收一个 topic 的一个 partition 的数据。而并不会增加处理数据时的并行度。不过度展开了，有兴趣请移步：[揭开Spark Streaming神秘面纱② - ReceiverTracker 与数据导入](http://www.jianshu.com/p/3195fb3c4191)
* 对于一个 topic，可以使用多个 groupid 相同的 input DStream 来使用多个 Receivers 来增加并行度，然后 union 他们；对于多个 topics，除了可以用上个办法增加并行度外，还可以对不同的 topic 使用不同的 input DStream 然后 union 他们来增加并行度
* 如果你启用了 WAL，为能将接收到的数据将以 log 的方式在指定的存储系统备份一份，需要指定输入数据的存储等级为 ```StorageLevel.MEMORY_AND_DISK_SER``` 或 ```StorageLevel.MEMORY_AND_DISK_SER_2```

##方式二：Without Receiver
自 Spark-1.3.0 起，提供了不需要 Receiver 的方法。替代了使用 receivers 来接收数据，该方法定期查询每个 topic+partition 的 lastest offset，并据此决定每个 batch 要接收的 offsets 范围。需要注意的是，该特性在 Spark-1.3（Scala API）是实验特性。

该方式相比使用 Receiver 的方式有以下好处：

* 简化并行：不再需要创建多个 kafka input DStream 然后再 union 这些 input DStream。使用 directStream，Spark Streaming会创建与 Kafka partitions 相同数量的 paritions 的 RDD，RDD 的 partition与 Kafka 的 partition 一一对应，这样更易于理解及调优
* 高效：在方式一中要保证数据零丢失需要启用 WAL（预写日志），这会占用更多空间。而在方式二中，可以直接从 Kafka 指定的 topic 的指定 offsets 处恢复数据，不需要使用 WAL
* 恰好一次语义保证：第一种方式使用了 Kafka 的 high level API 来在 Zookeeper 中存储已消费的 offsets。这在某些情况下会导致一些数据被消费两次，比如 streaming app 在处理某个 batch 内已接受到的数据的过程中挂掉，但是数据已经处理了一部分，但这种情况下无法将已处理数据的 offsets 更新到 Zookeeper 中，下次重启时，这批数据将再次被消费且处理。方式二中，只要将 output 操作和保存 offsets 操作封装成一个原子操作就能避免失败后的重复消费和处理，从而达到恰好一次的语义（Exactly-once）

当然，方式二相比于方式一也有缺陷，即不会自动更新消费的 offsets 至 Zookeeper，从而一些监控工具就无法看到消费进度。方式二需要自行保存消费的 offsets，这在 topic 新增 partition 时会变得更加麻烦。

下面来说说怎么使用方式二，示例如下：

```
 import org.apache.spark.streaming.kafka._

 val directKafkaStream = KafkaUtils.createDirectStream[
     [key class], [value class], [key decoder class], [value decoder class] ](
     streamingContext, [map of Kafka parameters], [set of topics to consume])
```

Kafka 参数中，需要指定 ```metadata.broker.list``` 或 ```bootstrap.servers```。默认会从每个 topic 的每个 partition 的 lastest offset 开始消费，也可以通过将 ```auto.offset.reset``` 设置为 ```smallest``` 来从每个 topic 的每个 partition 的 smallest offset 开始消费。

使用其他重载的 ```KafkaUtils.createDirectStream``` 函数也支持从任意 offset 消费数据。另外，如果你想在每个 bath 内获取消费的 offset，可以按下面的方法做：

```
// Hold a reference to the current offset ranges, so it can be used downstream
 var offsetRanges = Array[OffsetRange]()
	
 directKafkaStream.transform { rdd =>
   offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
   rdd
 }.map {
           ...
 }.foreachRDD { rdd =>
   for (o <- offsetRanges) {
     println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
   }
   ...
 }
```

你可以用上面的方法获取 offsets 并保存到Zookeeper或数据库中等。

需要注意的是，RDD partition 与 Kafka partition 的一一对应关系在shuffle或repartition之后将不复存在（ 如reduceByKey() 或 window() ），所以要获取 offset 需要在此之前。

另一个需要注意的是，由于方式二不使用 Receiver，所以任何 Receiver 相关的配置，即```spark.streaming.receiver.*```均不生效，需要转而使用 ```spark.streaming.kafka.*```。一个重要的参数是 ```spark.streaming.kafka.maxRatePerPartition```，用来控制每个 partition 每秒能接受的数据条数的上限。

##参考
1. http://spark.apache.org/docs/latest/streaming-kafka-integration.html

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
