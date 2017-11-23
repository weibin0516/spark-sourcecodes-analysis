前文[揭开Spark Streaming神秘面纱③ - 动态生成 job
](http://www.jianshu.com/p/ee845802921e)我们分析了 JobScheduler 是如何动态为每个 batch生成 jobs，本文将说明这些生成的 jobs 是如何被提交的。

在 JobScheduler 生成某个 batch 对应的 Seq[Job] 之后，会将 batch 及 Seq[Job] 封装成一个 JobSet 对象，JobSet 持有某个 batch 内所有的 jobs，并记录各个 job 的运行状态。

之后，调用```JobScheduler#submitJobSet(jobSet: JobSet)```来提交 jobs，在该函数中，除了一些状态更新，主要任务就是执行

```
jobSet.jobs.foreach(job => jobExecutor.execute(new JobHandler(job)))
```

即，对于 jobSet 中的每一个 job，执行```jobExecutor.execute(new JobHandler(job))```，要搞懂这行代码干了什么，就必须了解 JobHandler 及 jobExecutor。

##JobHandler
JobHandler 继承了 Runnable，为了说明与 job 的关系，其精简后的实现如下：

```
private class JobHandler(job: Job) extends Runnable with Logging {
  import JobScheduler._

  def run() {
    _eventLoop.post(JobStarted(job))
    PairRDDFunctions.disableOutputSpecValidation.withValue(true) {
      job.run()
    }
    _eventLoop = eventLoop
    if (_eventLoop != null) {
      _eventLoop.post(JobCompleted(job))
    }
  }

}
```

```JobHandler#run``` 方法主要执行了 ```job.run()```，该方法最终将调用到   
[揭开Spark Streaming神秘面纱③ - 动态生成 job
](http://www.jianshu.com/p/ee845802921e)
 中的『生成该 batch 对应的 jobs的Step2 定义的 jobFunc』，jonFunc 将提交对应 RDD DAG 定义的 job。

##JobExecutor
知道了 JobHandler 是用来执行 job 的，那么 JobHandler 将在哪里执行 job 呢？答案是
jobExecutor，jobExecutor为 JobScheduler 成员，是一个线程池，在JobScheduler 主构造函数中创建，如下：

```
private val numConcurrentJobs = ssc.conf.getInt("spark.streaming.concurrentJobs", 1)
private val jobExecutor = ThreadUtils.newDaemonFixedThreadPool(numConcurrentJobs, "streaming-job-executor")
```

JobHandler 将最终在 线程池jobExecutor 的线程中被调用，jobExecutor的线程数可通过```spark.streaming.concurrentJobs```配置，默认为1。若配置多个线程，就能让多个 job 同时运行，若只有一个线程，那么同一时刻只能有一个 job 运行。

以上，即 jobs 被执行的逻辑。

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
