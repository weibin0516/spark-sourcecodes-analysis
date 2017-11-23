###职责
 * 等待DAGScheduler job完成，一个JobWaiter对象与一个job唯一一一对应
 * 一旦task完成，将该task结果填充到```SparkContext.runJob```创建的results数组中

###构造函数

```
private[spark] class JobWaiter[T](
    dagScheduler: DAGScheduler,
    val jobId: Int,
    totalTasks: Int,
    resultHandler: (Int, T) => Unit)
  extends JobListener {...}
```

在SparkContext.runJob中，通过

```
val results = new Array[U](partitions.size)
runJob[T, U](rdd, func, partitions, allowLocal, (index, res) => results(index) = res)
```
来创建容纳job结果的数据，数组的每个元素对应与之下标相等的partition的计算结果；并将结果处理函数```(index, res) => results(index) = res```作为参数传入runJob，以使在runJob内部的创建的JobWaiter对象能够在得知taskSucceeded之后，将该task的结果填充到results中

###重要成员及方法
```
private var finishedTasks = 0
```
已经完成的task个数

---

```
private var jobResult: JobResult = if (jobFinished) JobSucceeded else null
```
如果job完成，jobResult为job的执行结果。对于0个task的job，直接设置job执行结果为JobSucceeded。

---

```
  def cancel() {
    
    dagScheduler.cancelJob(jobId)
  }
```
发送一个信号来取消job。该取消操作本身会被异步执行。在TaskScheduler取消所有属于该job的tasks后，该job会以一个Spark异常结束。

---

```
override def taskSucceeded(index: Int, result: Any): Unit = synchronized { ... }
```

* 讲该task结果，即参数result，填充到SparkContext.runJob中建立的```val results = new Array[U](partitions.size)```中
* ```finishedTasks += 1```，判断finishedTasks是否与totalTasks相等，若相等，则```_jobFinished = true jobResult = JobSucceeded```
      
***问：***什么情况下会 taskSucceeded 方法会被调用？
***答：***DAGScheduler收到```completion @ CompletionEvent```事件后，会调用```dagScheduler.handleTaskCompletion(completion)```，该函数会最终调用```job.listener.taskSucceeded(rt.outputId, event.result)```，job.listener为trait JobListener对象，具体实现为JobWaiter

--- 

```def awaitResult(): JobResult = synchronized { ... }```
等待job结束，并返回jobResult

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
