本文旨在说明 Spark 的延迟调度及其是如何工作的

##什么是延迟调度
在 Spark 中，若 task 与其输入数据在同一个 jvm 中，我们称 task 的本地性为 ```PROCESS_LOCAL```，这种本地性（locality level）是最优的，避免了网络传输及文件 IO，是最快的；其次是 task 与输入数据在同一节点上的 ```NODE_LOCAL```，数据在哪都一样的 ```NO_PREF```，数据与 task 在同一机架不同节点的 ```RACK_LOCAL``` 及最糟糕的不在同一机架的 ```ANY```。

本地性越好，对于 task 来说，花在网络传输及文件 IO 的时间越少，整个 task 执行耗时也就更少。而对于很多 task 来说，执行 task 的时间往往会比网络传输/文件 IO 的耗时要短的多。所以 Spark 希望尽量以更优的本地性启动 task。延迟调度就是为此而存在的。

在[Spark的位置优先(1): TaskSetManager 的有效 Locality Levels](http://www.jianshu.com/p/05034a9c8cae)这篇文章中，我们可以知道，假设一个 task 的最优本地性为 N，那么该 task 同时也具有其他所有本地性比 N 差的本地性。

假设调度器上一次以 locality level（本地性） M 为某个 taskSetManager 启动 task 失败，则说明该 taskSetManager 中包含本地性 M 的 tasks 的本地性 M 对应的所有节点均没有空闲资源。此时，只要当期时间与上一次以 M 为 taskSetManager 启动 task 时间差小于配置的值，调度器仍然会以 locality level M 来为 taskSetManager 启动 task

##延时调度如何工作

函数```TaskSetManager#getAllowedLocalityLevel```是实现延时调度最关键的地方，用来返回**当前该 taskSetManager 中未执行的 tasks 的最高可能 locality level**。以下为其实现

```
/**
   * Get the level we can launch tasks according to delay scheduling, based on current wait time.
   */
  private def getAllowedLocalityLevel(curTime: Long): TaskLocality.TaskLocality = {
    // Remove the scheduled or finished tasks lazily
    def tasksNeedToBeScheduledFrom(pendingTaskIds: ArrayBuffer[Int]): Boolean = {
      var indexOffset = pendingTaskIds.size
      while (indexOffset > 0) {
        indexOffset -= 1
        val index = pendingTaskIds(indexOffset)
        if (copiesRunning(index) == 0 && !successful(index)) {
          return true
        } else {
          pendingTaskIds.remove(indexOffset)
        }
      }
      false
    }
    // Walk through the list of tasks that can be scheduled at each location and returns true
    // if there are any tasks that still need to be scheduled. Lazily cleans up tasks that have
    // already been scheduled.
    def moreTasksToRunIn(pendingTasks: HashMap[String, ArrayBuffer[Int]]): Boolean = {
      val emptyKeys = new ArrayBuffer[String]
      val hasTasks = pendingTasks.exists {
        case (id: String, tasks: ArrayBuffer[Int]) =>
          if (tasksNeedToBeScheduledFrom(tasks)) {
            true
          } else {
            emptyKeys += id
            false
          }
      }
      // The key could be executorId, host or rackId
      emptyKeys.foreach(id => pendingTasks.remove(id))
      hasTasks
    }

    while (currentLocalityIndex < myLocalityLevels.length - 1) {
      val moreTasks = myLocalityLevels(currentLocalityIndex) match {
        case TaskLocality.PROCESS_LOCAL => moreTasksToRunIn(pendingTasksForExecutor)
        case TaskLocality.NODE_LOCAL => moreTasksToRunIn(pendingTasksForHost)
        case TaskLocality.NO_PREF => pendingTasksWithNoPrefs.nonEmpty
        case TaskLocality.RACK_LOCAL => moreTasksToRunIn(pendingTasksForRack)
      }
      if (!moreTasks) {
        // This is a performance optimization: if there are no more tasks that can
        // be scheduled at a particular locality level, there is no point in waiting
        // for the locality wait timeout (SPARK-4939).
        lastLaunchTime = curTime
        logDebug(s"No tasks for locality level ${myLocalityLevels(currentLocalityIndex)}, " +
          s"so moving to locality level ${myLocalityLevels(currentLocalityIndex + 1)}")
        currentLocalityIndex += 1
      } else if (curTime - lastLaunchTime >= localityWaits(currentLocalityIndex)) {
        // Jump to the next locality level, and reset lastLaunchTime so that the next locality
        // wait timer doesn't immediately expire
        lastLaunchTime += localityWaits(currentLocalityIndex)
        currentLocalityIndex += 1
        logDebug(s"Moving to ${myLocalityLevels(currentLocalityIndex)} after waiting for " +
          s"${localityWaits(currentLocalityIndex)}ms")
      } else {
        return myLocalityLevels(currentLocalityIndex)
      }
    }
    myLocalityLevels(currentLocalityIndex)
  }
```

代码有点小长，好在并不复杂，一些关键注释在以上源码中都有注明。

循环条件为```while (currentLocalityIndex < myLocalityLevels.length - 1) ```，
其中```myLocalityLevels: Array[TaskLocality.TaskLocality]```是当前 TaskSetManager 的所有 tasks 所包含的本地性（locality）集合，本地性越高的 locality level 在 myLocalityLevels 中的下标越小（具体请参见http://www.jianshu.com/p/05034a9c8cae）

currentLocalityIndex 是 getAllowedLocalityLevel 前一次返回的 locality level 在 myLocalityLevels 中的索引（下标），若 getAllowedLocalityLevel 是第一次被调用，则 currentLocalityIndex 为0

整个循环体都在做这几个事情：

1. 判断 ```myLocalityLevels(currentLocalityIndex)``` 这个级别的本地性对应的待执行 tasks 集合中是否还有待执行的 task
2. 若无；则将 ```currentLocalityIndex += 1``` 进行下一次循环，即将 locality level 降低一级回到第1步
3. 若有，且当前时间与上次getAllowedLocalityLevel返回 ```myLocalityLevels(currentLocalityIndex)``` 时间间隔小于 ```myLocalityLevels(currentLocalityIndex)``` 对应的延迟时间（通过```spark.locality.wait.process或spark.locality.wait.node或spark.locality.wait.rack```配置），则 currentLocalityIndex 不变，返回myLocalityLevels(currentLocalityIndex)。这里是延迟调度的关键，只要当前时间与上一次以某个 locality level 启动 task 的时间只差小于配置的值，不管上次是否成功启动了 task，这一次仍然以上次的 locality level 来启动 task。说的更明白一些：比如上次以 localtyX 为 taskSetManager 启动 task 失败，说明taskSetManager 中 tasks 对应 localityX 的节点均没有空闲资源来启动 task，但 Spark 此时仍然会以 localityX 来为 taskSetManager 启动 task。为什么要这样做？一般来说，task 执行耗时相对于网络传输/文件IO 要小得多，调度器多等待1 2秒可能就可以以更好的本地性执行 task，避免了更耗时的网络传输或文件IO，task 整体执行时间会降低
4. 若有，且当前时间与上次getAllowedLocalityLevel返回 ```myLocalityLevels(currentLocalityIndex)``` 时间间隔大于 ```myLocalityLevels(currentLocalityIndex)``` 对应的延迟时间，则将 ```currentLocalityIndex += 1``` 进行下一次循环，即将 locality level 降低一级回到第1步
	
	---
	
下面为帮助理解代码的部分说明
	
###判断是否还有当前 locality level 的 task 需要执行

```
val moreTasks = myLocalityLevels(currentLocalityIndex) match {
    case TaskLocality.PROCESS_LOCAL => moreTasksToRunIn(pendingTasksForExecutor)
    case TaskLocality.NODE_LOCAL => moreTasksToRunIn(pendingTasksForHost)
    case TaskLocality.NO_PREF => pendingTasksWithNoPrefs.nonEmpty
    case TaskLocality.RACK_LOCAL => moreTasksToRunIn(pendingTasksForRack)
  }
```
moreTasksToRunIn就不进行过多解释了，主要作用有两点：

1. 对于不同等级的 locality level 的 tasks 列表，将已经成功执行的或正在执行的该 locality level 的 task 从对应的列表中移除
2. 判断对应的 locality level 的 task 是否还要等待执行的，若有则返回 true，否则返回 false

以 ```myLocalityLevels(currentLocalityIndex)``` 等于 ```PROCESS_LOCAL``` 为例，这一段代码用来判断该 taskSetManager 中的 tasks 是否还有 task 的 locality levels 包含 ```PROCESS_LOCAL```

###if (!moreTasks)
若!moreTasks，则对currentLocalityIndex加1，即 locality level 变低一级，再次循环。

根据 http://www.jianshu.com/p/05034a9c8cae 的分析我们知道，若一个 task 存在于某个 locality level 为 level1 待执行 tasks 集合中，那么该 task 也一定存在于所有 locality level 低于 level1 的待执行 tasks 集合。

从另一个角度看，对于每个 task，总是尝试以最高的 locality level 去启动，若启动失败且下次以该 locality 启动时间与上次以该 locality level 启动时间超过配置的值，则将 locality level 降低一级来尝试启动 task

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
