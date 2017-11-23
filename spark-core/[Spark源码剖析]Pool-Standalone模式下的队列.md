#Pool-Spark Standalone模式下的队列

```org.apache.spark.scheduler.Pool```是 Spark Standalone 模式下的队列。从其重要成员及成员函数来剖析这个在 TaskScheduler 调度中起关键作用的类。

##成员
下图展示了 Pool 的所有成员及一些简要说明


![](http://upload-images.jianshu.io/upload_images/204749-fd1634c486cdc591.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

其中，```taskSetSchedulingAlgorithm```的类型由```schedulingMode```决定，下文会对```FairSchedulingAlgorithm```和```FIFOSchedulingAlgorithm```做详细分析

```
  var taskSetSchedulingAlgorithm: SchedulingAlgorithm = {
    schedulingMode match {
      case SchedulingMode.FAIR =>
        new FairSchedulingAlgorithm()
      case SchedulingMode.FIFO =>
        new FIFOSchedulingAlgorithm()
    }
  }
```

##成员函数
先来看看如何向一个 Pool 中添加 TaskSetManager 或 Pool，说明都写在注释中。

```
  override def addSchedulable(schedulable: Schedulable) {
  	//<f 判断 schedulable 不为 null
    require(schedulable != null)
    //< 往队列中添加schedulable 对象，可以是taskSet，也可以是子队列
    schedulableQueue.add(schedulable)
    schedulableNameToSchedulable.put(schedulable.name, schedulable)
    //< 将该 schedulable 对象的父亲设置为自己
    schedulable.parent = this
  }
```

以下为如何 remove 一个 TaskSetManager 或 Pool，需要注意的是schedulableQueue为```ConcurrentLinkedQueue```类型，其 remove 方法可以删除与参数值相等的元素

```
  override def removeSchedulable(schedulable: Schedulable) {
    schedulableQueue.remove(schedulable)
    schedulableNameToSchedulable.remove(schedulable.name)
  }
```

当有 executor 丢失时，会调用 executorLost 方法

```
  override def executorLost(executorId: String, host: String) {
    schedulableQueue.foreach(_.executorLost(executorId, host))
  }
```

若该队列中某个元素为 TaskSetManager 类型，会调用 ```TaskSetManager.executorLost``` 方法，该方法将查找是否有自己管理的 task 在 lost 的 executor 上运行，若有，则重新将该 lost 的 task 插入队列，等待执行；若某元素为 Pool 类型，即子队列，那么 ```Pool.executorLost``` 方法会对其schedulableQueue的所有元素调用 executorLost 方法，这样一来，若根 Pool 调用 executorLost 方法，则该队列下的所有 TaskSetManager 对象都能调用 executorLost 方法，那么因某个 executor lost 而 lost 的 task 都将被重新插入队列执行

```getSortedTaskSetQueue```方法是 Pool 最重要的方法，它将以该 Pool 为根队列的所有 TaskSetManager 排序后存在一个数组中，下标越小的数组越早被执行。代码如下：

```
  override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {
    var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
    val sortedSchedulableQueue =
      schedulableQueue.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator)
    for (schedulable <- sortedSchedulableQueue) {
      sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue
    }
    sortedTaskSetQueue
  }
```

这个函数的实现逻辑主要分为两步，假设现在调用 ```tmpPool.getSortedTaskSetQueue```，tmpPool 为 Pool 类型：

1. 对 tmpPool 的直接子 Pool 和 TaskSetManager 进行排序，排序的算法根据Pool 的 schedulingMode 而定，FAIR 和 FIFO 不相同。排序后得到sortedSchedulableQueue
2. 遍历sortedSchedulableQueue所有元素。若元素为 TaskSetManager 类型，则将该元素添加到```sortedTaskSetQueue: ArrayBuffer[TaskSetManager]```尾部，若为 Pool 类型，则执行第一步
3. 返回包含对 tmpPool 下所有 TaskSetManager 排序过后的数组

经过这几部，就能将一个 Pool 下的所有 TaskSetManager 排序，也就能确定哪个 TaskSetManager 的 tasks 要优先被 TaskScheduler 调度。

如上所述，排序的关键是```taskSetSchedulingAlgorithm.comparator```，上文中已经提到```taskSetSchedulingAlgorithm```根据schedulingMode值的不同，可以有```FairSchedulingAlgorithm```和```FIFOSchedulingAlgorithm```两种类型。先来看
FIFOSchedulingAlgorithm的排序

```
private[spark] class FIFOSchedulingAlgorithm extends SchedulingAlgorithm {
  override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
    val priority1 = s1.priority
    val priority2 = s2.priority
    var res = math.signum(priority1 - priority2)
    if (res == 0) {
      val stageId1 = s1.stageId
      val stageId2 = s2.stageId
      res = math.signum(stageId1 - stageId2)
    }
    if (res < 0) {
      true
    } else {
      false
    }
  }
}
```

FIFOSchedulingAlgorithm比较逻辑很简单，可概括为下面两句话：

1. 首先比较优先级值，优先级值越小的更优先（好拗口）
2. 若优先级值相等，则比较 stageId 值，stageId 值越小的越优先

FairSchedulingAlgorithm的比较逻辑会复杂一些，代码如下：

```
private[spark] class FairSchedulingAlgorithm extends SchedulingAlgorithm {
  override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
    val minShare1 = s1.minShare
    val minShare2 = s2.minShare
    val runningTasks1 = s1.runningTasks
    val runningTasks2 = s2.runningTasks
    val s1Needy = runningTasks1 < minShare1
    val s2Needy = runningTasks2 < minShare2
    val minShareRatio1 = runningTasks1.toDouble / math.max(minShare1, 1.0).toDouble
    val minShareRatio2 = runningTasks2.toDouble / math.max(minShare2, 1.0).toDouble
    val taskToWeightRatio1 = runningTasks1.toDouble / s1.weight.toDouble
    val taskToWeightRatio2 = runningTasks2.toDouble / s2.weight.toDouble
    var compare: Int = 0

    if (s1Needy && !s2Needy) {
      //< s1中正在执行的 tasks 个数小于 s1的最小 cpu 核数；且s2中正在执行的 tasks 个数等于 s2的最小 cpu 核数。则 s1优先
      return true
    } else if (!s1Needy && s2Needy) {
      //< s2中正在执行的 tasks 个数小于 s2的最小 cpu 核数；且s1中正在执行的 tasks 个数等于 s1的最小 cpu 核数。则 s2优先
      return false
    } else if (s1Needy && s2Needy) {
      //< s1,s2中正在执行的 tasks 个数小于其最小 cpu 核数。则比较各自 runningTasks1.toDouble / math.max(minShare1, 1.0).toDouble 的比值，小的优先
      compare = minShareRatio1.compareTo(minShareRatio2)
    } else {
      //< s1,s2中正在执行的 tasks 个数等于其最小 cpu 核数。则比较runningTasks1.toDouble / s1.weight.toDouble，小的优先
      compare = taskToWeightRatio1.compareTo(taskToWeightRatio2)
    }

    if (compare < 0) {
      true
    } else if (compare > 0) {
      false
    } else {
      //< 若以上比较都相等，则比较 s1和 s2的名字
      s1.name < s2.name
    }
  }
}
```

FairSchedulingAlgorithm的比较规则以在上面代码的注释中说明

##PS
Pool 的成员stageId 初始值为-1，但搜遍整个 Spark 源码也没有找到哪里有对该值的重新赋值。这个 stageId 的具体含义及如何发挥作用还没有完全搞明白，若哪位朋友知道，麻烦告知，多谢

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
