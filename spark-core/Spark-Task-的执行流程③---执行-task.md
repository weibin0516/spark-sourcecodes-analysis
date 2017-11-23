> 本文为 Spark 2.0 源码分析笔记，其他版本可能稍有不同

[创建、分发 Task](http://www.jianshu.com/p/08c66cbc31d6)一文中我们提到 ```TaskRunner```（继承于 Runnable） 对象最终会被提交到 ```Executor``` 的线程池中去执行，本文就将对该执行过程进行剖析。

该执行过程封装在 ```TaskRunner#run()``` 中，搞懂该函数就搞懂了 task 是如何执行的，按照本博客惯例，这里必定要来一张该函数的核心实现：


![](http://upload-images.jianshu.io/upload_images/204749-f0505fcc393c3369.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


需要注意的是，上图的流程都是在 Executor 的线程池中的某条线程中执行的。上图中最复杂和关键的是 ```task.run(...)``` 以及任务结果的处理，也即怎么把各个 partition 计算结果汇报到 driver 端。

task 结果处理这一块内容将另写一篇文章进行说明，下文主要对 ```task.run(...)``` 进行分析。Task 类共有两种实现：

* ResultTask：对于 DAG 图中最后一个 Stage（也就是 ResultStage），会生成与该 DAG 图中哦最后一个 RDD （DAG 图中最后边）partition 个数相同的 ResultTask
* ShuffleMapTask：对于非最后的 Stage（也就是 ShuffleMapStage），会生成与该 Stage 最后的 RDD partition 个数相同的 ShuffleMapTask

在 ```Task#run(...)``` 方法中最重要的是调用了 ```Task#runTask(context: TaskContext)``` 方法，来分别看看 ResultTask 和 ShuffleMapTask 的实现：

## ResultTask#runTask(context: TaskContext)

```
  override def runTask(context: TaskContext): U = {
    // Deserialize the RDD and the func using the broadcast variables.
    val deserializeStartTime = System.currentTimeMillis()
    val ser = SparkEnv.get.closureSerializer.newInstance()
    //< 反序列化得到 rdd 及 func
    val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime

    //< 对 rdd 指定 partition 的迭代器执行 func 函数
    func(context, rdd.iterator(partition, context))
  }
```

实现代码如上，主要做了两件事：

1. 反序列化得到 rdd 及 func
2. 对 rdd 指定 partition 的迭代器执行 func 函数并返回结果

func 函数是什么呢？我举几个例子就很容易明白：

* 对于 ```RDD#count()``` 的 ResultTask  这里的 func 真正执行的是 ```def getIteratorSize[T](iterator: Iterator[T]): Long```，即计算该 partition 对应的迭代器的数据条数
* 对于 ```RDD#take(num: Int): Array[T]``` 的 ResultTask 这里的 func 真正执行的是 ```(it: Iterator[T]) => it.take(num).toArray```，即取该 partition 对应的迭代器的前 num 条数据

也就是说，func 是对已经计算获得的 RDD 的某个 partition 的迭代器执行在 RDD action 中预定义好的操作，具体的操作根据不同的 action 不同而不同。而这个 partition 对应的迭代器的获取是通过调动 ```RDD#iterator(split: Partition, context: TaskContext): Iterator[T]``` 去获取的，会通过计算或从 cache 或 checkpoint 中获取。

## ShuffleMapTask#runTask(context: TaskContext)
与 ResultTask 对 partition 数据进行计算得到计算结果并汇报给 driver 不同，ShuffleMapTask 的职责是为下游的 RDD 计算出输入数据。更具体的说，ShuffleMapTask 要计算出 partition 数据并通过 shuffle write 写入磁盘（由 BlockManager 来管理）来等待下游的 RDD 通过 shuffle read 读取，其核心流程如下：


![](http://upload-images.jianshu.io/upload_images/204749-2f39dcde143b3e4c.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


共分为四步：

1. 从 SparkEnv 中获取 ShuffleManager 对象，当前支持 Hash、Sort Based、Tungsten-sort Based 以及自定义的 Shuffle（关于 shuffle 之后会专门写文章说明）
2. 从 ShuffleManager 中获取 ShuffleWriter 对象 writer
3. 得到对应 partition 的迭代器后，通过 writer 将数据写入文件系统中
4. 停止 writer 并返回结果

---

参考：《Spark 技术内幕》

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
