> 本文基于Spark 1.3.1，Standalone模式

一个Spark Application分为stage级别和task级别的调度，stage级别的调度已经用[DAGScheduler划分stage]和[DAGScheduler提交stage]两片文章进行源码层面的说明，本文将从源码层面剖析task是如何被调度和执行的。

##函数调用流程
先给出task调度的总体函数调用流程，并说明每个关键函数是干嘛的。这样一开始就在心里有个大概的流程图，便于之后的理解。

```
//< DAGScheduler调用该taskScheduler.submitTasks提交一个stage对应的taskSet，一个taskSet包含多个task
TaskSchedulerImpl.submitTasks(taskSet: TaskSet)
	//< TaskScheduler（实际上是TaskSchedulerImpl）为DAGScheduler提交的每个taskSet创建一个对应的TaskSetManager对象，TaskSetManager用于调度同一个taskSet中的task
	val manager = TaskSchedulerImpl.createTaskSetManager(taskSet, maxTaskFailures)
	//< 将新创建的manager加入到调度树中，调度树由SchedulableBulider维护。有FIFO、Fair两种实现
	SchedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)
		//< 触发调用CoarseGrainedSchedulerBackend.reviveOffers()，它将通过发送事件触发makeOffers方法调用
		CoarseGrainedSchedulerBackend.reviveOffers()
			//< 此处为发送ReviveOffers事件
			driverEndpoint.send(ReviveOffers)
		//< 此处为接收事件并处理
		CoarseGrainedSchedulerBackend.receive 
			CoarseGrainedSchedulerBackend.makeOffers
				//< 查找各个节点空闲资源（这里是cores），并返回要在哪些节点上启动哪些tasks的对应关系，用Seq[Seq[TaskDescription]]表示
				TaskSchedulerImpl.resourceOffers
			//< 启动对应task
			CoarseGrainedSchedulerBackend.launchTasks
				executorData.executorEndpoint.send(LaunchTask(new SerializableBuffer(serializedTask)))
```

看了上述流程可能不那么明白，没关系，不明白才要往下看。

##TaskSchedulerImpl.submitTasks(...)

在Spark 1.3.1版本中，TaskSchedulerImpl是TaskScheduler的唯一实现。submitTasks函数主要作用如下源码及注释所示：

1. 为taskSet创建对应的TaskSetManager对象。TaskManager的主要功能在于对Task的细粒度调度，比如
	* 决定在某个executor上是否启动及启动哪个task
	* 为了达到Locality aware，将Task的调度做相应的延迟
	* 当一个Task失败的时候，在约定的失败次数之内时，将Task重新提交
	* 处理拖后腿的task
2. 调用SchedulerBackend.makeOffers进入下一步

```
override def submitTasks(taskSet: TaskSet) {
  val tasks = taskSet.tasks
  logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")
  this.synchronized {
    //< 为stage对应的taskSet创建TaskSetManager对象
    val manager = createTaskSetManager(taskSet, maxTaskFailures)
    //< 建立taskset与TaskSetManager的对应关系
    activeTaskSets(taskSet.id) = manager

    //< TaskSetManager会被放入调度池（Pool）当中。
	  schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)

    //< 设置定时器，若task还没启动，则一直输出未分配到资源报警（输出警告日志）
    if (!isLocal && !hasReceivedTask) { 
      starvationTimer.scheduleAtFixedRate(new TimerTask() {
        override def run() {
          if (!hasLaunchedTask) {
            logWarning("Initial job has not accepted any resources; " +
              "check your cluster UI to ensure that workers are registered " +
              "and have sufficient resources")
          } else {
            this.cancel()
          }
        }
      }, STARVATION_TIMEOUT_MS, STARVATION_TIMEOUT_MS)
    }
    hasReceivedTask = true
  }

  //< 将处触发调用SchedulerBackend.makeOffers来为tasks分配资源，调度任务
  backend.reviveOffers()
}
```

## 基于事件模型的调用

下面源码及注释展示了CoarseGrainedSchedulerBackend是如何通过事件模型来进一步调用的。其中ReviveOffers事件有两种触发模式：

1. 周期性触发的，默认1秒一次
2. reviveOffers被TaskSchedulerImpl.reviveOffers()调用

```
  override def reviveOffers() {
    driverEndpoint.send(ReviveOffers)
  }
  
  override def receive: PartialFunction[Any, Unit] = {
  //< 此处省略n行代码
   
  case ReviveOffers =>
    makeOffers()

  //< 此处省略n行代码
  }
```

## CoarseGrainedSchedulerBackend.makeOffers()
该函数非常重要，它将集群的资源以Offer的方式发给上层的TaskSchedulerImpl。TaskSchedulerImpl调用scheduler.resourceOffers获得要被执行的Seq[TaskDescription]，然后将得到的Seq[TaskDescription]交给CoarseGrainedSchedulerBackend分发到各个executor上执行

```
    def makeOffers() {
      launchTasks(scheduler.resourceOffers(executorDataMap.map { case (id, executorData) =>
        new WorkerOffer(id, executorData.executorHost, executorData.freeCores)
      }.toSeq))
    }
```

为便于理解makeOffers调用及间接调用的各个流程，将该函数实现分为三个step来分析，这需要对源码的表现形式做一点点改动，但并不会有任何影响。

###Step1: val seq = executorDataMap.map { case (id, executorData) => new WorkerOffer(id, executorData.executorHost, executorData.freeCores) }.toSeq

executorDataMap是HashMap[String, ExecutorData]类型，在该HashMap中key为executor id，value为ExecutorData类型（包含executor的host，RPC信息，TotalCores，FreeCores信息）

```
//< 代表一个executor上的可用资源（这里仅可用cores）
private[spark]
case class WorkerOffer(executorId: String, host: String, cores: Int)
```



这段代码，返回HashMap[executorId, WorkerOffer]。每个WorkerOffer包含executor的id，host及其上可用cores信息。

###Step2: val taskDescs = scheduler.resourceOffers( seq )
拿到集群里的executor及其对应WorkerOffer后，就要开始第二个步骤，即找出要在哪些Worker上启动哪些task。这个过程比较长，也比较复杂。让我来一层层拨开迷雾。

我把```val taskDescs = scheduler.resourceOffers( seq )```即```TaskSchedulerImpl.resourceOffers(offers: Seq[WorkerOffer])```，返回的是Seq[Seq[TaskDescription]] 类型，来看看其实现：

```
def resourceOffers(offers: Seq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {
    //< 标记每个slave为alive并记录它们的hostname
    var newExecAvail = false
    //< 此处省略更新executor，host，rack信息代码；这里会根据是否有新的executor更新newExecAvail的值
  
    //< 为了负载均衡，打乱offers顺序，Random.shuffle用于将一个集合中的元素打乱
    val shuffledOffers = Random.shuffle(offers)
    //< 事先创建好用于存放要在各个worker上launch的 List[workerId, ArrayBuffer[TaskDescription]]。
    //< 由于task要使用的cores并不一定为1，所以每个worker上要launch得task并不一定等于可用的cores数
    val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores))
    //< 每个executor上可用的cores
    val availableCpus = shuffledOffers.map(o => o.cores).toArray

    //< 返回排序过的TaskSet队列，有FIFO及Fair两种排序规则，默认为FIFO，可通过配置修改
    val sortedTaskSets = rootPool.getSortedTaskSetQueue
    for (taskSet <- sortedTaskSets) {
      //< 如果有新的executor added，更新TaskSetManager可用的executor
      if (newExecAvail) {
        taskSet.executorAdded()
      }
    }

    var launchedTask = false
    //< 依次取出排序过的taskSet列表中的taskSet；
    //< 对于每个taskSet，取出其tasks覆盖的所有locality，从高到低依次遍历每个等级的locality；
    //< 取出了taskSet及本次要处理的locality后，根据该taskSet及locality遍历所有可用的worker，找出可以在各个worker上启动的task，加到tasks:Seq[Seq[TaskDescription]]中
    for (taskSet <- sortedTaskSets; maxLocality <- taskSet.myLocalityLevels) {
      do {
        //< 获取tasks，tasks代表要在哪些worker上启动哪些tasks
        launchedTask = resourceOfferSingleTaskSet(
            taskSet, maxLocality, shuffledOffers, availableCpus, tasks)
      } while (launchedTask)
    }

    if (tasks.size > 0) {
      hasLaunchedTask = true
    }
    return tasks
  }
```

结合代码，概括起来说，Step2又可以分为4个SubStep:

* 【SubStep1】: executor, host, rack等信息更新
* 【SubStep2】: 随机打乱workers。目的是为了分配tasks能负载均衡，分配tasks时，是从打乱的workers的序列的0下标开始判断是否能在worker上启动task的
* 【SubStep3】: RootPool对它包含的所有的TaskSetManagers进行排序并返回已排序的TaskSetManager数组。这里涉及到RootPool概念及如何排序，将会在下文展开说明
* 【SubStep4】: 对于RootPool返回的排序后的ArrayBuffer[TaskSetManager]中的每一个TaskSetManager，取出其包含的tasks包含的所有locality。根据locality从高到低，对于每个locality，遍历所有worker，结合延迟调度机制，判断TaskSetManager的哪些tasks可以在哪些workers上启动。这里比较需要进一步说明的是“延迟调度机制”及如何判断某个TaskSetManager里的tasks是否有可以在某个worker上启动

下面，就对SubStep3及SubStep4进行展开说明

####【SubStep3】
SubStep3的职责是"RootPool对它包含的所有的TaskSetManagers进行排序并返回已排序的TaskSetManager数组"。那么什么是RootPool呢？每个Spark Application包含唯一一个TaskScheduler对象，该TaskScheduler对象包含唯一一个RootPool，Spark Application包含的所有Job的所有stage对应的所有未完成的TaskSetManager都会保存在RootPool中，完成后从RootPool中remove。RootPool为```org.apache.spark.scheduler.Pool```类型，称作调度池。Pool的概念与YARN中队列的概念比较类似，一个队列可以包含子队列，相对的一个Pool可以包含子Pool；YARN队列的叶子节点即提交到该队列的Application，Pool的叶子节点即分配到该Pool的TaskSetManager。Pool根据调度模式的不同，分为FIFO及Fair。FIFO模式下只有一层Pool，不同于YARN的队列可以n多层，Pool的Fair调度模式下，只能有三层：RootPool，RootPool的子Pools，子Pools的叶子节点（即TaskSetManager）。

不同的调度模式添加叶子节点的实现是一样的，如下：

```
  override def addSchedulable(schedulable: Schedulable) {
    require(schedulable != null)
    //< 当我们添加一个元素的时候，它会添加到队列的尾部，当我们获取一个元素时，它会返回队列头部的元素
    schedulableQueue.add(schedulable)
    schedulableNameToSchedulable.put(schedulable.name, schedulable)
    schedulable.parent = this
  }
```

Schedulable类型的参数schedulable包含成员```val parent: Pool```，即父Pool，所以在添加TaskSetManager到Pool的时候就指定了父Pool。对于FIFO，所有的TaskSetManager的父Pool都是RootPool；对于Fair，TaskSetManager的父Pool即RootPool的某个子Pool。

不同的模式，除了Pool的层级结构不同，对它包含的TaskSetManagers进行排序时使用的算法也不同。FIFO对应FIFOSchedulingAlgorithm类，Fair对应FairSchedulingAlgorithm()类

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

当Pool.getSortedTaskSetQueue被调用时，就会用到该排序类，如下：

```
  //< 利用排序算法taskSetSchedulingAlgorithm先对以本pool作为父pool的子pools做排序，再对排序后的pool中的每个TaskSetManager排序；
  //< 得到最终排好序的 ArrayBuffer[TaskSetManager]
  override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {
    var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
    val sortedSchedulableQueue =
      schedulableQueue.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator)
    //< FIFO不会调到这里，直接走到下面的return  
    for (schedulable <- sortedSchedulableQueue) {
      sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue
    }
    sortedTaskSetQueue
  }
```

FIFO排序类中的比较函数的实现很简单：
1. Schedulable A和Schedulable B的优先级，优先级值越小，优先级越高
2. A优先级与B优先级相同，若A对应stage id越小，优先级越高

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

Pool及TaskSetManager都继承于Schedulable，来看下它的定义：

```
private[spark] trait Schedulable {
  var parent: Pool
  // child queues
  def schedulableQueue: ConcurrentLinkedQueue[Schedulable]
  def schedulingMode: SchedulingMode
  def weight: Int
  def minShare: Int
  def runningTasks: Int
  def priority: Int
  def stageId: Int
  def name: String

	//< 省略若干代码
}
```

可以看到，Schedulable包含weight（权重）、priority（优先级）、minShare（最小共享量）等属性。其中：
* weight：权重，默认是1，设置为2的话，就会比其他调度池获得2x多的资源，如果设置为-1000，该调度池一有任务就会马上运行
* minShare：最小共享核心数，默认是0，在权重相同的情况下，minShare大的，可以获得更多的资源

对于Fair调度模式下的比较，实现如下：

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
    var compare:Int = 0

    if (s1Needy && !s2Needy) {
      return true
    } else if (!s1Needy && s2Needy) {
      return false
    } else if (s1Needy && s2Needy) {
      compare = minShareRatio1.compareTo(minShareRatio2)
    } else {
      compare = taskToWeightRatio1.compareTo(taskToWeightRatio2)
    }

    if (compare < 0) {
      true
    } else if (compare > 0) {
      false
    } else {
      s1.name < s2.name
    }
  }
}
```

结合以上代码，我们可以比较容易看出Fair调度模式的比较逻辑：
1. 正在运行的task个数小于最小共享核心数的要比不小于的优先级高
2. 若两者正在运行的task个数都小于最小共享核心数，则比较minShare使用率的值，即```runningTasks.toDouble / math.max(minShare, 1.0).toDouble```，越小则优先级越高
3. 若minShare使用率相同，则比较权重使用率，即```runningTasks.toDouble / s.weight.toDouble```，越小则优先级越高
4. 如果权重使用率还相同，则比较两者的名字

对于Fair调度模式，需要先对RootPool的各个子Pool进行排序，再对子Pool中的TaskSetManagers进行排序，使用的算法都是```FairSchedulingAlgorithm.FairSchedulingAlgorithm```。

到这里，应该说清楚了整个SubStep3的流程。

####SubStep4
SubStep4说白了就是已经知道了哪些worker上由多少可用cores了，然后要决定要在哪些worker上启动哪些tasks：

```
//< 事先创建好用于存放要在各个worker上launch的 List[workerId, ArrayBuffer[TaskDescription]]。
//< 由于task要使用的cores并不一定为1，所以每个worker上要launch得task并不一定等于可用的cores数
val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores))

var launchedTask = false
for (taskSet <- sortedTaskSets; maxLocality <- taskSet.myLocalityLevels) {
  do {
    //< 获取tasks，tasks代表要在哪些worker上启动哪些tasks
    launchedTask = resourceOfferSingleTaskSet(
        taskSet, maxLocality, shuffledOffers, availableCpus, tasks)
  } while (launchedTask)
}
```

从for循环可以看到，该过程对排好序的taskSet数组的每一个元素，从locality优先级从高到低（taskSet.myLocalityLevels返回该taskSet包含的所有task包含的locality，按locality从高到低排列，PROCESS_LOCAL最高）取出locality，以取出的taskSet和locality调用```TaskSchedulerImpl.resourceOfferSingleTaskSet```，来看下它的实现（为方便阅读及理解，删去一些代码）：

```
private def resourceOfferSingleTaskSet(
    taskSet: TaskSetManager,
    maxLocality: TaskLocality,
    shuffledOffers: Seq[WorkerOffer],
    availableCpus: Array[Int],
    tasks: Seq[ArrayBuffer[TaskDescription]]) : Boolean = {
  var launchedTask = false

  //< 获取每个worker上要执行的tasks序列
  for (i <- 0 until shuffledOffers.size) {
    val execId = shuffledOffers(i).executorId
    val host = shuffledOffers(i).host
    if (availableCpus(i) >= CPUS_PER_TASK) {
      try {
        for (task <- taskSet.resourceOffer(execId, host, maxLocality)) {
          //< 将获得要在index为i的worker上执行的task，添加到tasks(i)中；这样就知道了要在哪个worker上执行哪些tasks了
          tasks(i) += task

          availableCpus(i) -= CPUS_PER_TASK
          assert(availableCpus(i) >= 0)
          launchedTask = true
        }
      } catch {
        case e: TaskNotSerializableException =>
          return launchedTask
      }
    }
  }
  return launchedTask
}
```
resourceOfferSingleTaskSet拿到worker可用cores，taskSet和locality后
1. 遍历每个worker的可用cores，如果可用cores大于task需要的cores数（即CPUS_PER_TASK），进入2
2. 调用```taskSet.resourceOffer(execId, host, maxLocality)```获取可在指定executor上启动的task，若返回非空，把返回的task加到最终的```tasks: Seq[ArrayBuffer[TaskDescription]]```中，该结构保存要在哪些worker上启动哪些tasks
3. 减少2中分配了task的worker的可用cores及更新其他信息

从以上的分析中可以看出，要在某个executor上启动哪个task最终的实现在```TaskSetManager.resourceOffer```中，由于该函数比较长，我将函数分过几个过程来分析

首先来看第一段:

```
//< 如果资源是有locality特征的
if (maxLocality != TaskLocality.NO_PREF) {
  //< 获取当前taskSet允许执行的locality。getAllowedLocalityLevel随时间变化而变化
  allowedLocality = getAllowedLocalityLevel(curTime)
  //< 如果允许的locality级别低于maxLocality，则使用maxLocality覆盖允许的locality
  if (allowedLocality > maxLocality) {
    // We're not allowed to search for farther-away tasks
    //< 临时将允许的locality级别降低到资源允许的最高locality级别
    allowedLocality = maxLocality
  }
}
```

要判断task能否在worker上启动，除了空闲资源是否达到task要求外，还需要判断本地性，即locality。locality从高到低共分为PROCESS_LOCAL, NODE_LOCAL,RACK_LOCAL及ANY。若taskSet带有locality属性，则通过getAllowedLocalityLevel函数获得该taskSet能容忍的最低界别locality。

getAllowedLocalityLevel中：
1. 如果taskset刚刚被提交，taskScheduler开始第一轮对taskset中的task开始提交，那么当时currentLocalityIndex为0，直接返回可用的最好的本地性；如果是在以后的提交过程中，那么如果当前的等待时间超过了一个级别，就向后跳一个级别
2. getAllowedLocalityLevel方法返回的是当前这次调度中，能够容忍的最差的本地性级别，在后续步骤的搜索中就只搜索本地性比这个级别好的情况
3. 随着时间的推移，撇开maxLocality配置不谈，对于本地性的容忍程度越来越大。

继续返回```TaskSetManager.resourceOffer```中，获得taskSet能容忍的最差locality后，与maxLocality比较去较差的locality作为最终的
能容忍的最差locality。

进入第二段：

```
dequeueTask(execId, host, allowedLocality) match {
  case Some((index, taskLocality, speculative)) => {
    //< 进行各种信息更新操作
    
    addRunningTask(taskId)

    // We used to log the time it takes to serialize the task, but task size is already
    // a good proxy to task serialization time.
    // val timeTaken = clock.getTime() - startTime
    val taskName = s"task ${info.id} in stage ${taskSet.id}"
    sched.dagScheduler.taskStarted(task, info)
    return Some(new TaskDescription(taskId = taskId, attemptNumber = attemptNum, execId,
      taskName, index, serializedTask))
  }
  case _ =>
}
```

可以看到，第二段首先调用了函数```dequeueTask```，如果返回不为空，说明为指定的worker分配了task；这之后，进行各种信息更新，将taskId加入到runningTask中，并通知DAGScheduler，最后返回taskDescription。来看看```dequeueTask```的实现：

```
private def dequeueTask(execId: String, host: String, maxLocality: TaskLocality.Value)
    : Option[(Int, TaskLocality.Value, Boolean)] =
  {
    //< dequeueTaskFromList: 该方法获取list中一个可以launch的task，同时清除扫描过的已经执行的task。其实它从第二次开始首先扫描的一定是已经运行完成的task，因此是延迟清除
    // 同一个Executor，通过execId来查找相应的等待的task
    for (index <- dequeueTaskFromList(execId, getPendingTasksForExecutor(execId))) {
      return Some((index, TaskLocality.PROCESS_LOCAL, false))
    }

    // 通过主机名找到相应的Task,不过比之前的多了一步判断
    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NODE_LOCAL)) {
      for (index <- dequeueTaskFromList(execId, getPendingTasksForHost(host))) {
        return Some((index, TaskLocality.NODE_LOCAL, false))
      }
    }


    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NO_PREF)) {
      // Look for noPref tasks after NODE_LOCAL for minimize cross-rack traffic
      for (index <- dequeueTaskFromList(execId, pendingTasksWithNoPrefs)) {
        return Some((index, TaskLocality.PROCESS_LOCAL, false))
      }
    }

    // 通过Rack的名称查找Task
    if (TaskLocality.isAllowed(maxLocality, TaskLocality.RACK_LOCAL)) {
      for {
        rack <- sched.getRackForHost(host)
        index <- dequeueTaskFromList(execId, getPendingTasksForRack(rack))
      } {
        return Some((index, TaskLocality.RACK_LOCAL, false))
      }
    }

    // 查找那些preferredLocations为空的，不指定在哪里执行的Task来执行
    if (TaskLocality.isAllowed(maxLocality, TaskLocality.ANY)) {
      for (index <- dequeueTaskFromList(execId, allPendingTasks)) {
        return Some((index, TaskLocality.ANY, false))
      }
    }

    // find a speculative task if all others tasks have been scheduled
    // 最后没办法了，拖的时间太长了，只能启动推测执行了
    dequeueSpeculativeTask(execId, host, maxLocality).map {
      case (taskIndex, allowedLocality) => (taskIndex, allowedLocality, true)}
  }
```

从该实现可以看出，不管之前获得的能容忍的最差locality（即allowedLocality）有多低，每次```dequeueTask```都是以PROCESS_LOCAL->...->allowedLocality顺序来判断是否可以以该locality启动task，而并不是必须以allowedLocality启动task。这也增大了启动task的机会。

到这里应该大致说清楚了Step2中的各个流程。




###Step3: launchTasks( taskDescs )
得到要在哪些worker上启动哪些task后，将调用launchTasks来启动各个task，实现如下：

```
def launchTasks(tasks: Seq[Seq[TaskDescription]]) {
  for (task <- tasks.flatten) {
    val ser = SparkEnv.get.closureSerializer.newInstance()
    //< 序列化task
    val serializedTask = ser.serialize(task)
    //< 若序列化后的task的size大于等于Akka可用空间
    if (serializedTask.limit >= akkaFrameSize - AkkaUtils.reservedSizeBytes) {
      val taskSetId = scheduler.taskIdToTaskSetId(task.taskId)
      scheduler.activeTaskSets.get(taskSetId).foreach { taskSet =>
        try {
          var msg = "Serialized task %s:%d was %d bytes, which exceeds max allowed: " +
            "spark.akka.frameSize (%d bytes) - reserved (%d bytes). Consider increasing " +
            "spark.akka.frameSize or using broadcast variables for large values."
          msg = msg.format(task.taskId, task.index, serializedTask.limit, akkaFrameSize, AkkaUtils.reservedSizeBytes)
          //< 中止taskSet，标记为已完成；同时将该taskSet的状态置为isZombie（Zombie：僵尸）
          taskSet.abort(msg)
        } catch {
          case e: Exception => logError("Exception in error callback", e)
        }
      }
    }
    else {
      //< 若序列化后的task的size小于Akka可用空间，减去对应executor上的可用cores数并向对应的executor发送启动task消息
      val executorData = executorDataMap(task.executorId)
      executorData.freeCores -= scheduler.CPUS_PER_TASK
      executorData.executorEndpoint.send(LaunchTask(new SerializableBuffer(serializedTask)))
    }
  }
}
```

逻辑比较简单，先对task进行序列化，若序列化后的task的size大于等于akka可用空间大小，则taskSet标记为已完成并置为Zombie状态；若序列化后的task的size小于akka可用空间大小，则通过发送消息给对应executor启动task

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
