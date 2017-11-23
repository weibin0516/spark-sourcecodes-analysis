> based on spark-1.5.1 standalone mode

在Spark Application Web UI的 Stages tag 上，我们可以看到这个的表格，描述的是某个 stage 的 tasks 的一些信息，其中 Locality Level 一栏的值可以有 ```PROCESS_LOCAL、NODE_LOCAL、NO_PREF、RACK_LOCAL、ANY``` 几个值。这篇文章将从这几个值入手，从源码角度分析 TaskSetManager 的 Locality Levels


![](http://upload-images.jianshu.io/upload_images/204749-e71313dc210a07ea.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
这几个值在图中代表 task 的计算节点和 task 的输入数据的节点位置关系

* ```PROCESS_LOCAL```: 数据在同一个 JVM 中，即同一个 executor 上。这是最佳数据 locality。
* ```NODE_LOCAL```: 数据在同一个节点上。比如数据在同一个节点的另一个 executor上；或在 HDFS 上，恰好有 block 在同一个节点上。速度比 PROCESS_LOCAL 稍慢，因为数据需要在不同进程之间传递或从文件中读取
* ```NO_PREF```: 数据从哪里访问都一样快，不需要位置优先
* ```RACK_LOCAL```: 数据在同一机架的不同节点上。需要通过网络传输数据及文件 IO，比 NODE_LOCAL 慢
* ```ANY```: 数据在非同一机架的网络上，速度最慢

我们在上图中看到的其实是结果，即某个 task 计算节点与其输入数据的位置关系，下面将要挖掘Spark 的调度系统如何产生这个结果，这一过程涉及 RDD、DAGScheduler、TaskScheduler，搞懂了这一过程也就基本搞懂了 Spark 的 PreferredLocations（位置优先策略）

##RDD 的 PreferredLocations
我们知道，根据输入数据源的不同，RDD 可能具有不同的优先位置，通过 RDD 的以下方法可以返回指定 partition 的最优先位置：

```
protected def getPreferredLocations(split: Partition): Seq[String]
```

返回类型为 ```Seq[String]```，其实对应的是 ```Seq[TaskLocation]```，在返回前都会执行 ```TaskLocation#toString``` 方法。TaskLocation 是一个 trait，共有以三种实现，分别代表数据存储在不同的位置：

```
/**
 * 代表数据存储在 executor 的内存中，也就是这个 partition 被 cache到内存了
 */
private [spark]
case class ExecutorCacheTaskLocation(override val host: String, executorId: String)
  extends TaskLocation {
  override def toString: String = s"${TaskLocation.executorLocationTag}${host}_$executorId"
}

/**
 * 代表数据存储在 host 这个节点的磁盘上
 */
private [spark] case class HostTaskLocation(override val host: String) extends TaskLocation {
  override def toString: String = host
}

/**
 * 代表数据存储在 hdfs 上
 */
private [spark] case class HDFSCacheTaskLocation(override val host: String) extends TaskLocation {
  override def toString: String = TaskLocation.inMemoryLocationTag + host
}
```

* ExecutorCacheTaskLocation: 代表 partition 数据已经被 cache 到内存，比如 KafkaRDD 会将 partitions 都 cache 到内存，其 toString 方法返回的格式如 ```executor_$host_$executorId```
* HostTaskLocation：代表 partition 数据存储在某个节点的磁盘上（且不在 hdfs 上），其 toString 方法直接返回 host
* HDFSCacheTaskLocation：代表 partition 数据存储在 hdfs 上，比如从 hdfs 上加载而来的 HadoopRDD 的 partition，其 toString 方法返回的格式如 ```hdfs_cache_$host```

这样，我们就知道不同的 RDD 会有不同的优先位置，并且存储在不同位置的优先位置的字符串的格式是不同的，这在之后 TaskSetManager 计算 tasks 的最优本地性起了关键作用。

##DAGScheduler 生成 taskSet
DAGScheduler 通过调用 submitStage 来提交一个 stage 对应的 tasks，submitStage 会调用submitMissingTasks，submitMissingTasks 会以下代码来确定每个需要计算的 task 的preferredLocations，这里调用到了 RDD#getPreferredLocs，getPreferredLocs返回的 partition 的优先位置，就是这个 partition 对应的 task 的优先位置

```
val taskIdToLocations = try {
  stage match {
    case s: ShuffleMapStage =>
      partitionsToCompute.map { id => (id, getPreferredLocs(stage.rdd, id))}.toMap
    case s: ResultStage =>
      val job = s.resultOfJob.get
      partitionsToCompute.map { id =>
        val p = job.partitions(id)
        (id, getPreferredLocs(stage.rdd, p))
      }.toMap
  }
} catch { 
  ... 
}
```

这段调用返回的 ```taskIdToLocations: Seq[ taskId -> Seq[hosts] ]``` 会在submitMissingTasks生成要提交给 TaskScheduler 调度的 taskSet: Seq[Task[_]]时用到，如下，注意看注释：

```
val tasks: Seq[Task[_]] = try {
  stage match {
    case stage: ShuffleMapStage =>
      partitionsToCompute.map { id =>
        val locs = taskIdToLocations(id)
        val part = stage.rdd.partitions(id)
        //< 使用上述获得的 task 对应的优先位置，即 locs 来构造ShuffleMapTask
        new ShuffleMapTask(stage.id, stage.latestInfo.attemptId,
          taskBinary, part, locs, stage.internalAccumulators)
      }

    case stage: ResultStage =>
      val job = stage.resultOfJob.get
      partitionsToCompute.map { id =>
        val p: Int = job.partitions(id)
        val part = stage.rdd.partitions(p)
        val locs = taskIdToLocations(id)
        //< 使用上述获得的 task 对应的优先位置，即 locs 来构造ResultTask
        new ResultTask(stage.id, stage.latestInfo.attemptId,
          taskBinary, part, locs, id, stage.internalAccumulators)
      }
  }
} catch {
  ...
}
```

简而言之，在 DAGScheduler 为 stage 创建要提交给 TaskScheduler 调度执行的 taskSet 时，**对于 taskSet 中的每一个 task，其优先位置与其对应的 partition 对应的优先位置一致**

## 构造 TaskSetManager，确定 locality levels
在 DAGScheduler 向 TaskScheduler 提交了 taskSet 之后，TaskSchedulerImpl 会为每个 taskSet 创建一个 TaskSetManager 对象，该对象包含taskSet 所有 tasks，并管理这些 tasks 的执行，其中就包括计算 taskSetManager 中的 tasks 都有哪些locality levels，以便在调度和延迟调度 tasks 时发挥作用。

在构造 TaskSetManager 对象时，会调用```var myLocalityLevels = computeValidLocalityLevels()```来确定locality levels

```
private def computeValidLocalityLevels(): Array[TaskLocality.TaskLocality] = {
    import TaskLocality.{PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY}
    val levels = new ArrayBuffer[TaskLocality.TaskLocality]
    if (!pendingTasksForExecutor.isEmpty && getLocalityWait(PROCESS_LOCAL) != 0 &&
        pendingTasksForExecutor.keySet.exists(sched.isExecutorAlive(_))) {
      levels += PROCESS_LOCAL
    }
    if (!pendingTasksForHost.isEmpty && getLocalityWait(NODE_LOCAL) != 0 &&
        pendingTasksForHost.keySet.exists(sched.hasExecutorsAliveOnHost(_))) {
      levels += NODE_LOCAL
    }
    if (!pendingTasksWithNoPrefs.isEmpty) {
      levels += NO_PREF
    }
    if (!pendingTasksForRack.isEmpty && getLocalityWait(RACK_LOCAL) != 0 &&
        pendingTasksForRack.keySet.exists(sched.hasHostAliveOnRack(_))) {
      levels += RACK_LOCAL
    }
    levels += ANY
    logDebug("Valid locality levels for " + taskSet + ": " + levels.mkString(", "))
    levels.toArray
  }
```

这个函数是在解决4个问题：

1. taskSetManager 的 locality levels是否包含 ```PROCESS_LOCAL```
2. taskSetManager 的 locality levels是否包含 ```NODE_LOCAL```
3. taskSetManager 的 locality levels是否包含 ```NO_PREF```
4. taskSetManager 的 locality levels是否包含 ```RACK_LOCAL```

让我们来各个击破

###taskSetManager 的 locality levels是否包含 ```PROCESS_LOCAL```
关键代码：

```
  if (!pendingTasksForExecutor.isEmpty && getLocalityWait(PROCESS_LOCAL) != 0 &&
      pendingTasksForExecutor.keySet.exists(sched.isExecutorAlive(_))) {
    levels += PROCESS_LOCAL
  }
```

真正关键的其实是这段代码，其他两个判断都很简单

```
pendingTasksForExecutor.keySet.exists(sched.isExecutorAlive(_))
```

要搞懂这段代码，首先要搞明白下面两个问题

1. pendingTasksForExecutor是怎么来的，什么含义？
2. sched.isExecutorAlive(_)干了什么？

####pendingTasksForExecutor是怎么来的，什么含义？
pendingTasksForExecutor 在 TaskSetManager 构造函数中被创建，如下
```private val pendingTasksForExecutor = new HashMap[String, ArrayBuffer[Int]]```其中，key 为executoroId，value 为task index 数组。在 TaskSetManager 的构造函数中如下调用

```
  for (i <- (0 until numTasks).reverse) {
    addPendingTask(i)
  }
```

这段调用为 taskSetManager 中的优先位置类型为 ```ExecutorCacheTaskLocation```（这里通过 toString 返回的格式进行匹配） 的 tasks 调用 addPendingTask，addPendingTask 获取 task 的优先位置，即一个 ```Seq[String]```；再获得这组优先位置对应的 executors，从来反过来获得了 executor 对应 partition 缓存在其上内存的 tasks，即pendingTasksForExecutor

简单的说，**pendingTasksForExecutor保存着当前可用的 executor 对应的 partition 缓存在在其上内存中的 tasks 的映射关系**

####sched.isExecutorAlive(_)干了什么？
sched.isExecutorAlive的实现为：

```
  def TaskSchedulerImpl#isExecutorAlive(execId: String): Boolean = synchronized {
    activeExecutorIds.contains(execId)
  }
```

```activeExecutorIds: HashSet[String]```保存集群当前所有可用的 executor id（这里对 executor 的 free cores 个数并没有要求，可为0），每当 DAGScheduler 提交 taskSet 会触发 TaskScheduler 调用 resourceOffers 方法，该方法会更新当前可用的 executors 至 activeExecutorIds；当有 executor lost 的时候，TaskSchedulerImpl 也会调用 removeExecutor 来将 lost 的executor 从 activeExecutorIds 中去除

所有**isExecutorAlive就是判断参数中的 executor id 当前是否 active**

---

结合以上两段代码的分析，可以知道这行代码```pendingTasksForExecutor.keySet.exists(sched.isExecutorAlive(_))```的含义： **taskSetManager 的所有对应 partition 数据缓存在 executor 内存中的 tasks 对应的所有 executor，是否有任一 active，若有则返回 true；否则返回 false**

这样，也就知道了如何去判断一个 taskSetManager 对象的 locality levels 是否包含 PROCESS_LOCAL

###taskSetManager 的 locality levels是否包含 ```NODE_LOCAL```
有了上面对 PROCESS_LOCAL 的详细分析，这里对是否包含 NODE_LOCAL 只做简要分析。最关键代码

```pendingTasksForHost.keySet.exists(sched.hasExecutorsAliveOnHost(_))```，其中

* pendingTasksForHost: ```HashMap[String, ArrayBuffer[Int]]```类型，key 为 host，value 为 preferredLocations 包含该 host 的 tasks indexs 数组
* sched.hasExecutorsAliveOnHost(_): 
源码如下，其中executorsByHost为 ```HashMap[String, HashSet[String]]``` 类型，key 为 host，value 为该 host 上的 active executors

```
  def hasExecutorsAliveOnHost(host: String): Boolean = synchronized {
    executorsByHost.contains(host)
  }
```

这样，也就知道如何判断 taskSetManager 的 locality levels：taskSetManager 的所有 tasks 对应的所有 hosts，是否有任一是 tasks 的优先位置 hosts，若有返回 true；否则返回 false

###taskSetManager 的 locality levels是否包含 ```RACK_LOCAL```
关键代码：```pendingTasksForRack.keySet.exists(sched.hasHostAliveOnRack(_))```，其中

* pendingTasksForRack：```HashMap[String, ArrayBuffer[Int]]```类型，key为 rack，value 为优先位置所在的 host 属于该机架的 tasks
* sched.hasHostAliveOnRack(_)：源码如下，其中```hostsByRack: HashMap[String, HashSet[String]]```的 key 为 rack，value 为该 rack 上所有作为 taskSetManager 优先位置的 hosts

```
  def hasHostAliveOnRack(rack: String): Boolean = synchronized {
    hostsByRack.contains(rack)
  }
```

所以，判断 taskSetManager 的 locality levels 是否包含```RACK_LOCAL```的规则为：taskSetManager 的所有 tasks 的优先位置 host 所在的所有 racks 与当前 active executors 所在的机架是否有交集，若有则返回 true，否则返回 false

###taskSetManager 的 locality levels是否包含 ```NO_PREF```
关键代码如下：

```
    if (!pendingTasksWithNoPrefs.isEmpty) {
      levels += NO_PREF
    }
```

如果一个 RDD 的某些 partitions 没有优先位置（如是以内存集合作为数据源且 executors 和 driver不在同一个节点），那么这个 RDD action 产生的 taskSetManagers 的 locality levels 就包含 NO_PREF

对于所有的 taskSetManager 均包含 ANY

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
