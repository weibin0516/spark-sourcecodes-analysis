> 本文为 Spark 2.0 源码分析笔记，由于源码只包含 standalone 模式下完整的 executor 相关代码，所以本文主要针对 standalone 模式下的 executor 模块，文中内容若不特意说明均为 standalone 模式内容

[前一篇文章](http://www.jianshu.com/p/5dab83e94cac)简要介绍了 Spark 执行模块中几个主要的类以及 AppClient 是如何被创建的，这篇文章将详细的介绍 AppClient 向 Master 注册 Application 的过程，将主要从以下几个方面进行说明：

* 注册 Application 时机
* 注册 Application 的重试机制
* 注册行为细节

## 注册 Application 时机
简单来说，AppClient 向 Master 注册 Application 是在 SparkContext 构造时发生的，也就是 driver 一开始运行就立马向 Master 注册 Application。更具体的步骤可以如下图表示：


![](http://upload-images.jianshu.io/upload_images/204749-b600f0ffb4f63f4f.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


## 注册 Application 的重试机制
StandaloneAppClient 中有两个成员，分别是：```private val REGISTRATION_TIMEOUT_SECONDS = 20``` 和 ```private val REGISTRATION_RETRIES = 3```。 其中，```REGISTRATION_RETRIES``` 代表注册 Application 的最大重试次数，为3次；而 ```REGISTRATION_TIMEOUT_SECONDS``` 代表 StandaloneAppClient 在执行注册之后隔多少秒去获取注册结果，具体的流程如下：

1. ClientEndpoint 实例通过发送 ```RegisterApplication``` 消息给 Master 来向 Master 注册 Application
2. 隔 ```REGISTRATION_TIMEOUT_SECONDS``` 秒后检测 registered 标记，若其对应值为 true，则表明注册成功；否则，表明注册失败
    * Master 会在注册 Application 后向 AppClient 响应 ```RegisteredApplication``` 消息，AppClient 收到该消息会置 registered 对应值为 true
    * 若 Master 没有响应该消息，则 registered 一直为 false）
3. 若注册成功，注册流程结束；若注册失败：
    * 已尝试注册次数小于 ```REGISTRATION_RETRIES```，返回第一步再来一次
    * 已尝试注册次数等于 ```REGISTRATION_RETRIES```，结束注册流程，将 Application 标记为 dead，通过回调通知 SchedulerBackend Application dead

上面这一小段即时注册 Application 的重试机制，下面再来看看注册的一些细节

## 注册行为的细节

注册行为可以主要分为以下三步：

1. AppClient 发起注册
2. Master 接收并处理注册消息
3. AppClient 处理 Master 的注册响应消息

### Step1：AppClient 发起注册
AppClient 是通过向 Master 发送 ```RegisterApplication``` 消息进行注册的。该消息定义为一个 case class，其中 ```appDescription: ApplicationDescription``` 成员描述了要注册并启动一个怎么样的 Application（主要包含属性及资源信息），其定义如下：

```
private[spark] case class ApplicationDescription(
    name: String,                               //< Application 的名字
    maxCores: Option[Int],                      //< application 总共能用的最大 cores 数量
    memoryPerExecutorMB: Int,                   //< 每个 executor 分配的内存
    command: Command,                           //< 启动 executor 的 ClassName、所需参数、环境信息等启动一个 Java 进程的所有需要的信息；在 Standalone 模式下，类名就是 CoarseGrainedExecutorBackend
    appUiUrl: String,                           //< Application 的 web ui 的 host:port
    eventLogDir: Option[URI] = None,            //< Spark事件日志记录的目录。在这个基本目录下，Spark为每个 Application 创建一个子目录。各个应用程序记录日志到相应的目录。常设置为 hdfs 目录以便于 history server 访问来重构 web ui的目录
    eventLogCodec: Option[String] = None,
    coresPerExecutor: Option[Int] = None,       //< 每个 executor 使用的 cores 数量
    initialExecutorLimit: Option[Int] = None,
    user: String = System.getProperty("user.name", "<unknown>")) {

  override def toString: String = "ApplicationDescription(" + name + ")"
}

private[spark] case class Command(
    mainClass: String,
    arguments: Seq[String],
    environment: Map[String, String],
    classPathEntries: Seq[String],
    libraryPathEntries: Seq[String],
    javaOpts: Seq[String]) {
}
```

除了 Application 的描述，注册时还会带上 ClientEndpoint 对应的 rpcEndpointRef，以便 Master 能通过该 rpcEndpointRef 给自身发送消息。

构造该消息实例后，ClientEndpoint 就会通过 master rpcEndpointRef 给 Master 发送该注册消息

### Step2：Master 接收并处理注册消息
Master 接收到注册消息后的主要处理流程如下图所示：

![](http://upload-images.jianshu.io/upload_images/204749-dd9a570a42529073.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


在向 driver 发送 RegisteredApplication 消息后，其实已经完成了注册流程，从上面的流程图可以看出，只要接收到 AppClient 的注册请求，Master 都能成功注册 Application 并响应消息。这之后的调度都做了什么呢？我们继续跟进 Master#schedule() 方法。

schedule() 的流程如下：

1. 打散（shuffle）所有状态为 ALIVE 的 workders
2. 对于每一个处于 WAITTING 状态的 driver，都要遍历所有的打散的 alive works
    * 如果 worker 的 free memory 和 free cores 都大于等于 driver 要求的值，则通过给该 worker 发送 ```LaunchDriver``` 消息来启动 driver 并把该 driver 从  WAITTING driver 中除名
3. ```startExecutorsOnWorkers()```：在 workers 上启动 executors（当前，只实现了简单的 FIFO 调度，先满足第一个 app，然后再满足第二个 app，以此类推）
    1. 从 waitingApps 中取出一个 app（app.coresLeft > 0）
    2. 对于该 app，从所有可用的 workers 中筛选出 free memory和 free cores 满足 app executor 需求的 worker，为 usableWorkers
    3. 调用 ```scheduleExecutorsOnWorkers``` 方法来在 usableWorkers 上分配 executors，有两种模式：
         * 一种是尽量把一个 app 的 executors 分配到尽可能多的 workers 上
         * 另一种是尽量把一个 app 的 executors 分配到尽量少的 workers 上
4. 上一步得到了要在每个 workers 上使用多少个 cores，这一步就要来分配这些了：
    * 调用 ```allocateWorkerResourceToExecutors``` 进行分配：
        * 分配一个 worker 的资源给一个或多个 executors
        * 调用 ```launchExecutor(worker, exec)``` 启动 executor
            * 对应的 WorkerInfo 增加刚分配的 ExecutorDesc
            * 给 worker 发送 LaunchExecutor 消息，以要求其启动指定信息的 executor
            * 给 driver 发送 ExecutorAdded 消息，以通知其有新的 Executor 添加了
        * 置 app 的状态为 RUNNING

### Step3：AppClient 处理 Master 的注册响应消息
Master 若成功处理了注册请求，会响应给 AppClient 一个 ```RegisteredApplication``` 消息，AppClient 在接收到该响应消息后，会进行一些简单的操作，主要包括：

* 设置 appId
* 至 registered 为 true
* 通知 SchedulerBackend 已成功注册 Application

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
