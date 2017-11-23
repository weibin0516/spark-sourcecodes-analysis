> 本文为 Spark 2.0 版本的源码分析，其他版本可能会有所不同

TaskScheduler 作为资源调度器的一个重要职责就在：

* 集群可用资源发生变化（比如有新增的 executor，有 executor lost 等）
* 有新的 task 提交
* 有 task 结束
* 处理 Speculatable task

等时机把处于等待状态的 tasks 分配给有空闲资源的 executors，那么这个 “把 task 分配给 executor” 的过程具体是怎样的呢？这就是本文要探讨的内容，将通过以下四小节来进行剖析：

1. 打散可用的 executors
2. 对所有处于等待状态的 taskSet 进行排序
3. 根据是否有新增的 executor 来决定是否更新各个 taskSet 的可用本地性集合
4. 结合 taskSets 的排序及本地性集合将 tasks 分配给 executors

## 打散可用的 executors
“把 task 分配给 executor” 这一过程是在函数 ```TaskSchedulerImpl#resourceOffers(offers: Seq[WorkerOffer]): Seq[Seq[TaskDescription]]``` 中完成的：

* 传入参数 ```offers: Seq[WorkerOffer]``` 为集群中所有 activeExecutors 一一对应，一个 WorkerOffer 包含的信息包括：executorId、executorHost及该 executor 空闲的 cores
* 返回值类型为 ```Seq[Seq[TaskDescription]]```，其每一个元素（即```Seq[TaskDescription]```类型）为经过该函数的调度分配给下标相同的 offers 元素对应的 executor 的 tasks

```TaskSchedulerImpl#resourceOffers``` 第一阶段做的事可用下图表示：


![](http://upload-images.jianshu.io/upload_images/204749-0d9a0e011193cdf9.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)



在该函数每次被调用之时，通过随机的方式打乱所有 workerOffers（一个 workerOffer 对应一个active executor），之后会根据这打乱后的顺序给 executor 分配 task，这样做就能避免只将 tasks 分配给少数几个 executors 从而达到使集群各节点压力平均的目的。

除了打散 workerOffers，还为每个 workerOffer 创建了一个可变的 TaskDescription 数组从而组成了一个二维数组 tasks，用于保存之后的操作中分配给各个 executor 的 tasks。

## 对所有处于等待状态的 taskSet 进行排序
排序的目的是为了让优先级更高的 taskSet 所包含的 task 更优先的被调度执行，所执行的操作是：

```
val sortedTaskSets: ArrayBuffer[TaskSetManager] = rootPool.getSortedTaskSetQueue
```

其中，```sortedTaskSets``` 是排序后得到的 ```TaskSetManager``` 数组，下标越小表示优先级越高，也就越优先被调度。而 ```rootPool``` 是 ```Pool``` 类型，它是 Standalone 模式下的对列，支持两种调度模式，分别是：

* SchedulingMode.FIFO：FIFO 模式，先进先出
* SchedulingMode.FAIR：公平模式，会考虑各个对列资源的使用情况

更具体的分析，请移步[Pool-Standalone模式下的队列](http://www.jianshu.com/p/f56c3fb989ad)，这篇文章对两种调度方式以及如何排序做做了十分详细的说明

## 根据是否有新增的 executor 来决定是否更新各个 taskSet 的可用本地性集合
关于更新 taskSet 的可用本地性集合，这里值进行简单说明，更多内容请移步 [Spark的位置优先: TaskSetManager 的有效 Locality Levels](http://www.jianshu.com/p/05034a9c8cae)

* 若 taskSet 中有 task 的 partition 是存储在 executor 内存中的且对应 executor alive，那么该 taskSet 的最佳本地性为 ```PROCESS_LOCAL```,可用本地性集合包括 ```PROCESS_LOCAL``` 及所有本地性比 ```PROCESS_LOCAL``` 查的，也就是该集合包括 ```PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY```
* 若 taskSet 中没有 task 的 partition 是存储在 executor 内存中的，但存在 partition 是存储在某个节点磁盘上的且对应节点 alive ，那么该 taskSet 的最佳本地性为 ```NODE_LOCAL```,可用本地性集合包括 ```NODE_LOCAL``` 及所有本地性比 ```NODE_LOCAL``` 查的，也就是该集合包括 ```NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY```
* 以此类推，可用本地性集合包含 taskSet 中的 tasks 所拥有的最佳本地性及所有比该本地性差的本地性

这个可用本地性集合会在后面的将 task 分配给 executor 起关键作用

## 结合 taskSets 的排序及本地性集合将 tasks 分配给 executors
这一步的实现代码如下：

```
for (taskSet <- sortedTaskSets; maxLocality <- taskSet.myLocalityLevels) {
  do {
    launchedTask = resourceOfferSingleTaskSet(
        taskSet, maxLocality, shuffledOffers, availableCpus, tasks)
  } while (launchedTask)
}
```

含义是根据 sortedTaskSets 的顺序依次遍历其每一个 taskSetManager，
从该 taskSetManager 的本地性从高到低依次调用 ```TaskSchedulerImpl#resourceOfferSingleTaskSet```，流程如下图：


![](http://upload-images.jianshu.io/upload_images/204749-3f3ad9aad7db6b08.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)



---


以上，就完成了分配 tasks 给 executors 的流程分析，细节比较多，涉及的知识点也比较多，需要扩展阅读文中给出的另几个文章，最后给出一个整体的流程图方便理解


![](http://upload-images.jianshu.io/upload_images/204749-c4f656cde319c120.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
