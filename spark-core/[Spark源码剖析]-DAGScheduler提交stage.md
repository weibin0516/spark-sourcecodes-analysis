DAGScheduler通过调用submitStage来提交stage，实现如下：

```
  private def submitStage(stage: Stage) {
    val jobId = activeJobForStage(stage)
    if (jobId.isDefined) {
      logDebug("submitStage(" + stage + ")")
      if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
        //< 获取该stage未提交的父stages，并按stage id从小到大排序
        val missing = getMissingParentStages(stage).sortBy(_.id)
        logDebug("missing: " + missing)
        if (missing.isEmpty) {
          logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
          //< 若无未提交的父stage, 则提交该stage对应的tasks
          submitMissingTasks(stage, jobId.get)
        } else {
          //< 若存在未提交的父stage, 依次提交所有父stage (若父stage也存在未提交的父stage, 则提交之, 依次类推); 并把该stage添加到等待stage队列中
          for (parent <- missing) {
            submitStage(parent)
          }
          waitingStages += stage
        }
      }
    } else {
      abortStage(stage, "No active job for stage " + stage.id)
    }
  }
```

submitStage先调用```getMissingParentStages```来获取参数stageX（这里为了区分，取名为stageX）是否有未提交的父stages，若有，则依次递归（按stage id从小到大排列，也就是stage是从后往前提交的）提交父stages，并将stageX加入到```waitingStages: HashSet[Stage]```中。对于要依次提交的父stage，也是如此。

```getMissingParentStages```与[DAGScheduler划分stage](http://blog.csdn.net/bigbigdata/article/details/47293263)中介绍的```getParentStages```有点像，但不同的是不再需要划分stage，并对每个stage的状态做了判断，源码及注释如下：

```
//< 以参数stage为起点，向前遍历所有stage，判断stage是否为未提交，若使则加入missing中
  private def getMissingParentStages(stage: Stage): List[Stage] = {
    //< 未提交的stage
    val missing = new HashSet[Stage]
    //< 存储已经被访问到得RDD
    val visited = new HashSet[RDD[_]]

    val waitingForVisit = new Stack[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visited(rdd)) {
        visited += rdd
        if (getCacheLocs(rdd).contains(Nil)) {
          for (dep <- rdd.dependencies) {
            dep match {
              //< 若为宽依赖，生成新的stage
              case shufDep: ShuffleDependency[_, _, _] =>
                //< 这里调用getShuffleMapStage不像在getParentStages时需要划分stage，而是直接根据shufDep.shuffleId获取对应的ShuffleMapStage
                val mapStage = getShuffleMapStage(shufDep, stage.jobId)
                if (!mapStage.isAvailable) {
                  // 若stage得状态为available则为未提交stage
                  missing += mapStage
                }
              //< 若为窄依赖，那就属于同一个stage。并将依赖的RDD放入waitingForVisit中，以能够在下面的while中继续向上visit，直至遍历了整个DAG图
              case narrowDep: NarrowDependency[_] =>
                waitingForVisit.push(narrowDep.rdd)
            }
          }
        }
      }
    }
    waitingForVisit.push(stage.rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    missing.toList
  }
```

上面提到，若stageX存在未提交的父stages，则先提交父stages；那么，如果stageX没有未提交的父stage呢（比如，包含从HDFS读取数据生成HadoopRDD的那个stage是没有父stage的）？

这时会调用```submitMissingTasks(stage, jobId.get)```，参数就是stageX及其对应的jobId.get。这个函数便是我们时常在其他文章或书籍中看到的将stage与taskSet对应起来，然后DAGScheduler将taskSet提交给TaskScheduler去执行的实施者。这个函数的实现比较长，下面分段说明。

###Step1: 得到RDD中需要计算的partition
对于Shuffle类型的stage，需要判断stage中是否缓存了该结果；对于Result类型的Final Stage，则判断计算Job中该partition是否已经计算完成。这么做（没有直接提交全部tasks）的原因是，stage中某个task执行失败其他执行成功的时候就需要找出这个失败的task对应要计算的partition而不是要计算所有partition

```
  private def submitMissingTasks(stage: Stage, jobId: Int) {
    stage.pendingTasks.clear()

    //< 首先得到RDD中需要计算的partition
    //< 对于Shuffle类型的stage，需要判断stage中是否缓存了该结果；
    //< 对于Result类型的Final Stage，则判断计算Job中该partition是否已经计算完成
    //< 这么做的原因是，stage中某个task执行失败其他执行成功地时候就需要找出这个失败的task对应要计算的partition而不是要计算所有partition
    val partitionsToCompute: Seq[Int] = {
      stage match {
        case stage: ShuffleMapStage =>
          (0 until stage.numPartitions).filter(id => stage.outputLocs(id).isEmpty)
        case stage: ResultStage =>
          val job = stage.resultOfJob.get
          (0 until job.numPartitions).filter(id => !job.finished(id))
      }
    }
```

###Step2: 序列化task的binary
Executor可以通过广播变量得到它。每个task运行的时候首先会反序列化

```
var taskBinary: Broadcast[Array[Byte]] = null
    try {
      // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
      // For ResultTask, serialize and broadcast (rdd, func).
      val taskBinaryBytes: Array[Byte] = stage match {
        case stage: ShuffleMapStage =>
          //< 对于ShuffleMapTask，将rdd及其依赖关系序列化；在Executor执行task之前会反序列化
          closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef).array()
          //< 对于ResultTask，对rdd及要在每个partition上执行的func
        case stage: ResultStage =>
          closureSerializer.serialize((stage.rdd, stage.resultOfJob.get.func): AnyRef).array()
      }

      //< 将序列化好的信息广播给所有的executor
      taskBinary = sc.broadcast(taskBinaryBytes)
    } catch {
      // In the case of a failure during serialization, abort the stage.
      case e: NotSerializableException =>
        abortStage(stage, "Task not serializable: " + e.toString)
        runningStages -= stage

        // Abort execution
        return
      case NonFatal(e) =>
        abortStage(stage, s"Task serialization failed: $e\n${e.getStackTraceString}")
        runningStages -= stage
        return
    }
```

###Step3: 为每个需要计算的partiton生成一个task
ShuffleMapStage对应的task全是ShuffleMapTask; ResultStage对应的全是ResultTask。task继承Serializable，要确保task是可序列化的。

```
val tasks: Seq[Task[_]] = stage match {
      case stage: ShuffleMapStage =>
        partitionsToCompute.map { id =>
          val locs = getPreferredLocs(stage.rdd, id)
          //< RDD对应的partition
          val part = stage.rdd.partitions(id)
          new ShuffleMapTask(stage.id, taskBinary, part, locs)
        }

      case stage: ResultStage =>
        val job = stage.resultOfJob.get
        //< id为输出分区索引，表示reducerID
        partitionsToCompute.map { id =>
          val p: Int = job.partitions(id)
          val part = stage.rdd.partitions(p)
          val locs = getPreferredLocs(stage.rdd, p)
          new ResultTask(stage.id, taskBinary, part, locs, id)
        }
    }
```

###Step4: 提交tasks
先用tasks来初始化一个TaskSet对象，再调用TaskScheduler.submitTasks提交

```
stage.pendingTasks ++= tasks
      logDebug("New pending tasks: " + stage.pendingTasks)
      //< 提交TaskSet至TaskScheduler
      taskScheduler.submitTasks(
        new TaskSet(tasks.toArray, stage.id, stage.newAttemptId(), stage.jobId, properties))
      //< 记录stage提交task的时间
      stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
    } else {
```

以上，介绍了提交stage和提交tasks的实现。本文若有纰漏，请批评指正。

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
