# 划分stage源码剖析

>本文基于Spark 1.3.1

先上一些stage相关的知识点：

1. DAGScheduler将Job分解成具有前后依赖关系的多个stage
2. DAGScheduler是根据ShuffleDependency划分stage的
3. stage分为ShuffleMapStage和ResultStage；一个Job中包含一个ResultStage及多个ShuffleMapStage
4. 一个stage包含多个tasks，task的个数即该stage的finalRDD的partition数
5. 一个stage中的task完全相同，ShuffleMapStage包含的都是ShuffleMapTask；ResultStage包含的都是ResultTask


下图为整个划分stage的函数调用关系图
![DAGScheduler划分stage函数调用关系.png](http://upload-images.jianshu.io/upload_images/204749-c24af52fc674f8d3.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


在DAGScheduler内部通过post一个JobSubmitted事件来触发Job的提交

```
DAGScheduler.eventProcessLoop.post( JobSubmitted(...) )
DAGScheduler.handleJobSubmitted
```

既然这两个方法都是DAGScheduler内部的实现，为什么不直接调用函数而要这样“多此一举”呢？我猜想这是为了保证整个系统事件模型的完整性。

DAGScheduler.handleJobSubmitted部分源码及如下

```
private[scheduler] def handleJobSubmitted(jobId: Int,
      finalRDD: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      allowLocal: Boolean,
      callSite: CallSite,
      listener: JobListener,
      properties: Properties) {
    var finalStage: ResultStage = null
    try {
      //< 创建finalStage可能会抛出一个异常, 比如, jobs是基于一个HadoopRDD的但这个HadoopRDD已被删除
      finalStage = newResultStage(finalRDD, partitions.size, jobId, callSite)
    } catch {
      case e: Exception => 
        return
    }
    
    //< 此处省略n行代码
  }
```

该函数通过调用newResultStage函数来创建唯一的ResultStage，也就是finalStage。调用newResultStage时，传入了finalRDD、partitions.size等参数。

跟进到```DAGScheduler.newResultStage```

```
  private def newResultStage(
      rdd: RDD[_],
      numTasks: Int,
      jobId: Int,
      callSite: CallSite): ResultStage = {
    val (parentStages: List[Stage], id: Int) = getParentStagesAndId(rdd, jobId)
    val stage: ResultStage = new ResultStage(id, rdd, numTasks, parentStages, jobId, callSite)

    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId, stage)
    stage
  }
```

DAGScheduler.newResultStage首先调用```val (parentStages: List[Stage], id: Int) = getParentStagesAndId(rdd, jobId)```，这个调用看起来像是要先确定好该ResultStage依赖的父stages，

***问题1：那么是直接父stage呢？还是父stage及间接依赖的所有父stage呢？***记住这个问题，继续往下看。

跟进到```DAGScheduler.getParentStagesAndId```:

```
  private def getParentStagesAndId(rdd: RDD[_], jobId: Int): (List[Stage], Int) = {
    val parentStages = getParentStages(rdd, jobId)
    val id = nextStageId.getAndIncrement()      //< 这个调用确定了每个stage的id，划分stage时，会从右到左，因为是递归调用，其实越左的stage创建时，越早调到?try
    (parentStages, id)
  }
```

该函数调用```getParentStages```获得parentStages，之后获取一个递增的id，连同刚获得的parentStages一同返回，并在newResultStage中，将id作为ResultStage的id。那么，
***问题2：stage id是父stage的大还是子stage的大？***。继续跟进源码，所有提问均会在后面解答。

跟到```getParentStages```里

```
  //< 这个函数的实现方式比较巧妙
  private def getParentStages(rdd: RDD[_], jobId: Int): List[Stage] = {
    //< 通过vist一级一级vist得到的父stage
    val parents = new HashSet[Stage]
    //< 已经visted的rdd
    val visited = new HashSet[RDD[_]]
    val waitingForVisit = new Stack[RDD[_]]
    def visit(r: RDD[_]) {
      if (!visited(r)) {
        visited += r

        for (dep <- r.dependencies) {
          dep match {
            //< 若为宽依赖，调用getShuffleMapStage
            case shufDep: ShuffleDependency[_, _, _] => 
              parents += getShuffleMapStage(shufDep, jobId)
            case _ =>
              //< 若为窄依赖，将该依赖中的rdd加入到待vist栈，以保证能一级一级往上vist，直至遍历整个DAG图
              waitingForVisit.push(dep.rdd)
          }
        }
      }
    }
    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    parents.toList
  }
```



函数getParentStages中，遍历整个RDD依赖图的finalRDD的List[dependency] （关于RDD及依赖，可参考[举例说明Spark RDD的分区、依赖](http://www.jianshu.com/p/6b9e4001723d)，若遇到ShuffleDependency（即宽依赖），则调用```getShuffleMapStage(shufDep, jobId)```返回一个```ShuffleMapStage```类型对象，添加到父stage列表中。若为NarrowDenpendency，则将该NarrowDenpendency包含的RDD加入到待visit队列中，之后继续遍历待visit队列中的RDD，直到遇到ShuffleDependency或无依赖的RDD。

函数```getParentStages```的职责说白了就是：以参数rdd为起点，一级一级遍历依赖，碰到窄依赖就继续往前遍历，碰到宽依赖就调用```getShuffleMapStage(shufDep, jobId)```。这里需要特别注意的是，```getParentStages```以rdd为起点遍历RDD依赖并不会遍历整个RDD依赖图，而是一级一级遍历直到所有“遍历路线”都碰到了宽依赖就停止。剩下的事，在遍历的过程中交给```getShuffleMapStage```。

那么，让我来看看函数```getShuffleMapStage```的实现：

```
private def getShuffleMapStage(
    shuffleDep: ShuffleDependency[_, _, _],
    jobId: Int): ShuffleMapStage = {
  shuffleToMapStage.get(shuffleDep.shuffleId) match {
    case Some(stage) => stage
    case None =>
      // We are going to register ancestor shuffle dependencies
      registerShuffleDependencies(shuffleDep, jobId)

      //< 然后创建新的ShuffleMapStage
      val stage = newOrUsedShuffleStage(shuffleDep, jobId)
      shuffleToMapStage(shuffleDep.shuffleId) = stage

      stage
  }
}
```

在划分stage的过程中，由于每次```shuffleDep.shuffleId```都不同且都是第一次出现，显然```shuffleToMapStage.get(shuffleDep.shuffleId)```会match到None，便会调用```newOrUsedShuffleStage```。来看看它的实现：

```
private def registerShuffleDependencies(shuffleDep: ShuffleDependency[_, _, _], jobId: Int) {
    val parentsWithNoMapStage = getAncestorShuffleDependencies(shuffleDep.rdd)
    while (parentsWithNoMapStage.nonEmpty) {
      //< 出栈的其实是shuffleDep的前一个宽依赖，且shuffleToMapStage不包含以该出栈宽依赖id为key的元素
      val currentShufDep = parentsWithNoMapStage.pop()
      //< 创建新的ShuffleMapStage
      val stage = newOrUsedShuffleStage(currentShufDep, jobId)
      //< 将新创建的ShuffleMapStage加入到shuffleId -> ShuffleMapStage映射关系中
      shuffleToMapStage(currentShufDep.shuffleId) = stage
    }
  }
```

函数```registerShuffleDependencies```首先调用```getAncestorShuffleDependencies```，这个函数遍历参数rdd的List[dependency]，若遇到ShuffleDependency，加入到```parents: Stack[ShuffleDependency[_, _, _]]```中；若遇到窄依赖，则遍历该窄依赖对应rdd的父一层依赖，知道遇到宽依赖为止。实现与```getParentStages```基本一致，不同的是这里是将宽依赖加入到parents中并返回。

registerShuffleDependencies拿到各个“依赖路线”最近的所有宽依赖后。对每个宽依赖调用```newOrUsedShuffleStage```，该函数用来创建新ShuffleMapStage或获得已经存在的ShuffleMapStage。来看它的实现：

```
private def newOrUsedShuffleStage(
      shuffleDep: ShuffleDependency[_, _, _],
      jobId: Int): ShuffleMapStage = {
    val rdd = shuffleDep.rdd
    val numTasks = rdd.partitions.size
    val stage = newShuffleMapStage(rdd, numTasks, shuffleDep, jobId, rdd.creationSite)
    //< 若该shuffleDep.shulleId对应的, stage已经在MapOutputTracker中存在，那么可用的输出的数量及位置将从MapOutputTracker恢复
    if (mapOutputTracker.containsShuffle(shuffleDep.shuffleId)) {
      val serLocs = mapOutputTracker.getSerializedMapOutputStatuses(shuffleDep.shuffleId)
      val locs = MapOutputTracker.deserializeMapStatuses(serLocs)
      for (i <- 0 until locs.size) {
        stage.outputLocs(i) = Option(locs(i)).toList // locs(i) will be null if missing
      }
      stage.numAvailableOutputs = locs.count(_ != null)
    } else {
      // Kind of ugly: need to register RDDs with the cache and map output tracker here
      // since we can't do it in the RDD constructor because # of partitions is unknown
      logInfo("Registering RDD " + rdd.id + " (" + rdd.getCreationSite + ")")
      //< 否则使用shuffleDep.shuffleId, rdd.partitions.size在mapOutputTracker中注册，这会在shuffle阶段reducer从shuffleMap端fetch数据起作用
      mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions.size)
    }
    stage
  }
```

函数```newOrUsedShuffleStage```首先调用```newShuffleMapStage```来创建新的ShuffleMapStage，来看下```newShuffleMapStage```的实现：

```
  private def newShuffleMapStage(
      rdd: RDD[_],
      numTasks: Int,
      shuffleDep: ShuffleDependency[_, _, _],
      jobId: Int,
      callSite: CallSite): ShuffleMapStage = {
    val (parentStages: List[Stage], id: Int) = getParentStagesAndId(rdd, jobId)
    val stage: ShuffleMapStage = new ShuffleMapStage(id, rdd, numTasks, parentStages,
      jobId, callSite, shuffleDep)

    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId, stage)
    stage
  }
```

结合文章开始处的函数调用关系图，可以看到```newShuffleMapStage```竟然又调用```getParentStagesAndId```来获取它的parentStages。那么，文章开头处的整个函数调用流程又会继续走一遍，不同的是起点rdd不是原来的finalRDD而是变成了这里的宽依赖的rdd。

静下心来，仔细看几遍上文提到的源码及注释，其实每一次的如上图所示的递归调用，其实就只做了两件事：
1. 遍历起点RDD的依赖列表，若遇到窄依赖，则继续遍历该窄依赖的父List[RDD]的依赖，直到碰到宽依赖；若碰到宽依赖（不管是起点RDD的宽依赖还是遍历多级依赖碰到的宽依赖），则以宽依赖RDD为起点再次重复上述过程。直到到达RDD依赖图的最左端，也就是遍历到了没有依赖的RDD，则进入2
2. 达到RDD依赖图的最左端，即递归调用也到了最深得层数，```getParentStagesAndId中```，```getParentStages```第一次返回（第一次返回为空，因为最初的stage没有父stage），```val id = nextStageId.getAndIncrement()```也是第一次被调用，获得第一个stage的id，为0（注意，这个时候还没有创建第一个stage）。这之后，便调用

```
val stage: ShuffleMapStage = new ShuffleMapStage(id, rdd, numTasks, parentStages, jobId, callSite, shuffleDep)
```

创建第一个ShuffleMapStage。至此，这一层递归调用结束，返回到上一层递归中，这一层创建的所有的ShuffleMapStage会作为下一层stage的父List[stage]用来构造上一层的stages。上一层递归调用返回后，上一层创建的stage又将作为上上层的parent List[stage]来构造上上层的stages。依次类推，直到最后的ResultStage也被创建出来为止。整个stage的划分完成。

有一个需要注意的点是，无论对于ShuffleMapStage还是ResultStage来说，task的个数即该stage的finalRDD的partition的个数，仔细查看下上文中的```newResultStage```和```newShuffleMapStage```函数可以搞明白这点，不再赘述。

最后，解答下上文中的两个问题：
***问题1：***每个stage都有一个val parents: List[Stage]成员，保存的是其直接依赖的父stages；其直接父stages又有自身依赖的父stages，以此类推，构成了整个DAG图

***问题2：***父stage的id比子stage的id小，DAG图中，越左边的stage，id越小。

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
