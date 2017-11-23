JobScheduler有两个重要成员，一是上文介绍的 ReceiverTracker，负责分发 receivers 及源源不断地接收数据；二是本文将要介绍的 JobGenerator，负责定时的生成 jobs 并 checkpoint。

##定时逻辑
在 JobScheduler 的主构造函数中，会创建 JobGenerator 对象。在 JobGenerator 的主构造函数中，会创建一个定时器：

```
  private val timer = new RecurringTimer(clock, ssc.graph.batchDuration.milliseconds,
    longTime => eventLoop.post(GenerateJobs(new Time(longTime))), "JobGenerator")
```

该定时器每隔 ```ssc.graph.batchDuration.milliseconds``` 会执行一次 ```eventLoop.post(GenerateJobs(new Time(longTime)))``` 向 eventLoop 发送 ```GenerateJobs(new Time(longTime))```消息，**eventLoop收到消息后会进行这个 batch 对应的 jobs 的生成及提交执行**，eventLoop 是一个消息接收处理器。
需要注意的是，timer 在创建之后并不会马上启动，将在 ```StreamingContext#start()``` 启动 Streaming Application 时间接调用到 ```timer.start(restartTime.milliseconds)```才启动。

##为 batch 生成 jobs


![](http://upload-images.jianshu.io/upload_images/204749-e6cd05d35d7031b9.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

eventLoop 在接收到 ```GenerateJobs(new Time(longTime))```消息后的主要处理流程有以上图中三步：

1. 将已接收到的 blocks 分配给 batch
2. 生成该 batch 对应的 jobs
3. 将 jobs 封装成 JobSet 并提交执行

接下来我们就将逐一展开这三步进行分析

###将已接受到的 blocks 分配给 batch

![](http://upload-images.jianshu.io/upload_images/204749-c85680875a7557c2.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

上图是根据源码画出的为 batch 分配 blocks 的流程图，这里对 『获得 batchTime 各个 InputDStream 未分配的 blocks』作进一步说明：
在文章 『文章链接』 中我们知道了各个 ReceiverInputDStream 对应的 receivers 接收并保存的 blocks 信息会保存在 ```ReceivedBlockTracker#streamIdToUnallocatedBlockQueues```，该成员 key 为 streamId，value 为该 streamId 对应的 InputDStream 已接收保存但尚未分配的 blocks 信息。
所以获取某 InputDStream 未分配的 blocks 只要以该 InputDStream 的 streamId 来从 streamIdToUnallocatedBlockQueues 来 get 就好。获取之后，会清楚该 streamId 对应的value，以保证 block 不会被重复分配。

在实际调用中，为 batchTime 分配 blocks 时，会从streamIdToUnallocatedBlockQueues取出未分配的 blocks 塞进 ```timeToAllocatedBlocks: mutable.HashMap[Time, AllocatedBlocks]``` 中，以在之后作为该 batchTime 对应的 RDD 的输入数据。

通过以上步骤，就可以为 batch 的所有 InputDStream 分配 blocks。也就是为 batch 分配了 blocks。

###生成该 batch 对应的 jobs


![](http://upload-images.jianshu.io/upload_images/204749-1a1227d30560e8eb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

为指定 batchTime 生成 jobs 的逻辑如上图所示。你可能会疑惑，为什么 ```DStreamGraph#generateJobs(time: Time)```为什么返回 ```Seq[Job]```，而不是单个 job。这是因为，在一个 batch 内，可能会有多个 OutputStream 执行了多次 output 操作，每次 output 操作都将产生一个 Job，最终就会产生多个 Jobs。

我们结合上图对执行流程进一步分析。
在```DStreamGraph#generateJobs(time: Time)```中，对于DStreamGraph成员ArrayBuffer[DStream[_]]的每一项，调用```DStream#generateJob(time: Time)```来生成这个 outputStream 在该 batchTime 的 job。该生成过程主要有三步：

####Step1: 获取该 outputStream 在该 batchTime 对应的 RDD
每个 DStream 实例都有一个 ```generatedRDDs: HashMap[Time, RDD[T]]``` 成员，用来保存该 DStream 在每个 batchTime 生成的 RDD，当 ```DStream#getOrCompute(time: Time)```调用时

* 首先会查看generatedRDDs中是否已经有该 time 对应的 RDD，若有则直接返回
* 若无，则调用```compute(validTime: Time)```来生成 RDD，这一步根据每个 InputDStream继承 compute 的实现不同而不同。例如，对于 FileInputDStream，其 compute 实现逻辑如下：
	
	1. 先通过一个 findNewFiles() 方法，找到多个新 file
	2. 对每个新 file，都将其作为参数调用 sc.newAPIHadoopFile(file)，生成一个 RDD 实例
	3. 将 2 中的多个新 file 对应的多个 RDD 实例进行 union，返回一个 union 后的 UnionRDD
	
####Step2: 根据 Step1中得到的 RDD 生成最终 job 要执行的函数 jobFunc
jobFunc定义如下：

```
val jobFunc = () => {
  val emptyFunc = { (iterator: Iterator[T]) => {} }
  context.sparkContext.runJob(rdd, emptyFunc)
}
```

可以看到，每个 outputStream 的 output 操作生成的 Job 其实与 RDD action 一样，最终调用 SparkContext#runJob 来提交 RDD DAG 定义的任务

####Step3: 根据 Step2中得到的 jobFunc 生成最终要执行的 Job 并返回
Step2中得到了定义 Job 要干嘛的函数-jobFunc，这里便以 jobFunc及 batchTime 生成 Job 实例：

```
Some(new Job(time, jobFunc))
```

该Job实例将最终封装在 JobHandler 中被执行

至此，我们搞明白了 JobScheduler 是如何通过一步步调用来动态生成每个 batchTime 的 jobs。下文我们将分析这些动态生成的 jobs 如何被分发及如何执行。

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
