> 本文为 Spark 2.0 源码分析笔记，由于源码只包含 standalone 模式下完整的 executor 相关代码，所以本文主要针对 standalone 模式下的 executor 模块，文中内容若不特意说明均为 standalone 模式内容

在 executor 模块中，最重要的几个类（或接口、trait）是：

* AppClient：在 Standalone 模式下的实现是 ```StandaloneAppClient``` 类
* SchedulerBackend：SchedulerBackend 是一个 trait，在 Standalone 模式下的实现是 ```StandaloneSchedulerBackend``` 类
* TaskScheduler：TaskScheduler 也是一个 trait，当前，在所有模式下的实现均为 ```TaskSchedulerImpl``` 类

接下来先简要介绍这几个类的作用以及各自主要的成员和方法，这是理解之后内容的基础

## StandaloneAppClient（AppClient）
StandaloneAppClient 主要有以下几个作用：

1. 向 master 注册 application
2. 接收并处理来自 master 的各种消息，如 ```RegisteredApplication```、```ApplicationRemoved```、```ExecutorAdded``` 等
3. 调用 SchedulerBackend 回调接口以通知各种重要的 event，比如：Application 失败、添加了 executor、executor 更新等

### 主要成员

* ```private val REGISTRATION_TIMEOUT_SECONDS = 20```：注册 application 的超时
* ```private val REGISTRATION_RETRIES = 3```：注册 application 的最大重试次数
* ```endpoint: ClientEndpoint```：ClientEndpoint 为 StandaloneAppClient 内部嵌套类，主要用来：
    * 通过向 master 发送 ```RegisterApplication``` 消息来注册 application
    * 接收来自 master 的消息并处理，消息包括
        * ```RegisteredApplication```：application 已成功注册
        * ```ApplicationRemoved```：application 已移除
        * ```ExecutorAdded```：有新增加的 Executor
        * ```ExecutorUpdated```：Executor 发生资源更新
        * ```MasterChanged```：master 改变
    * 接收来自 StandaloneAppClient 发送的消息并处理，包括：
        * ```StopAppClient```：StandaloneAppClient stop 时通知 ClientEndpoint 也进行 stop 并反注册 application
        * ```RequestExecutors```：StandaloneAppClient 在注册完 Application 后通过 ClientEndpoint 向 master 为执行 Application 的 tasks 申请资源
        * ```KillExecutors```：StandaloneAppClient 通过 ClientEndpoint 向 master 发送消息来 kill executor

### 主要方法
* ```def start()```：启动 StandaloneAppClient
* ```def requestTotalExecutors(requestedTotal: Int): Boolean```：为 application 向 master 申请指定总数的 executors
* ```def killExecutors(executorIds: Seq[String]): Boolean```：通过 ClientEndpoint 向 master 发送消息来 kill 一组 executors

## SchedulerBackend
SchedulerBackend 在 Standalone 模式下的 SchedulerBackend 的实现是 StandaloneSchedulerBackend，但是从大体的作用上来说，各个模式下的 SchedulerBackend 作用是相同的，主要为：

1. 当有新的 task 提交或资源更新时，查找各个节点空闲资源，并确定在哪个 executor 上启动哪个 task 的对应关系，对应的方法是 ```def reviveOffers(): Unit```
2. 被 TaskScheduler 调用来 kill task，对应的方法是 ```def killTask(...): Unit```

## TaskScheduler
低等级的 task 调度接口，当前只有 TaskSchedulerImpl 这一个实现。该接口支持在不同的部署模式下工作。每个 SparkContext(application) 对应唯一的一个 TaskScheduler。 TaskScheduler 从 DAGScheduler 的每一个 stage 获取 tasks，并负责发送到集群去执行这些 tasks，在失败的时候重试，并减轻掉队情况。TaskScheduler 会返回 events 给 DAGScheduler。

### 主要方法
* ```def rootPool: Pool```：返回 root 调度对列
* ```def schedulingMode: SchedulingMode```：调度模式
* ```def submitTasks(taskSet: TaskSet)```：提交任务去集群执行
* ```def cancelTasks(stageId: Int, interruptThread: Boolean)```：取消一个 stage 对应的 tasks
* ```def executorHeartbeatReceived(...) ```：接收到 executor 心跳信息
* ```def executorLost(executorId: String, reason: ExecutorLossReason)```：处理
executor lost

以上简要的介绍了 AppClient、SchedulerBackend、TaskScheduler 几个接口，其中 SchedulerBackend 和 TaskScheduler 接口实例是在 SparkContext 构造函数中创建的，而 AppClient 实例是在 SchedulerBackend 构造函数中被创建。

## AppClient 的创建与启动
AppClient 的创建与启动也比较简单，主要流程如下：

1. 在 SparkContext 的构造函数中，调用 ```val (sched, ts) = SparkContext.createTaskScheduler(this, master, deployMode)``` 来通过 master url 来创建相应模式下的 SchedulerBackend 实例 sched 以及 TaskSchedulerImpl 实例 ts（我们假定这里创建的 sched 是 StandaloneScheduler 类型的）
2. 随后，依然是在 SparkContext 的构造函数中，TaskScheduler 实例 ts 调用其 start 方法，在该 start 方法中会调用 SchedulerBackend 实例 sched 的 start 方法（所以，你也可以从这里知道 TaskScheduler 的实现中是包含 SchedulerBackend 的实例的）
3. 在 SchedulerBackend 的 start 方法中会创建其嵌套类 ClientEndpoint 对象
4. 在将 ClientEndpoint 对象注册给 rpcEnv 的过程中 ClientEndpoint 对象会收到 OnStart 消息并处理，处理过程主要就是持有 ApplicationDescription（主要包括name, maxCores, memoryPerExecutorMB, 启动命令行, appUiUrl等） 来向 Master 注册 application

再次说明，以上内容若无特别说明均指 Standalone 模式下的。本文简要的分析了几个关键类以及 AppClient 是如何启动的，更详细的剖析会在后面的文章中说明。

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
