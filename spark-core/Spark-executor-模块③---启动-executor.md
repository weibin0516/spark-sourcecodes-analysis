> 本文为 Spark 2.0 源码分析笔记，由于源码只包含 standalone 模式下完整的 executor 相关代码，所以本文主要针对 standalone 模式下的 executor 模块，文中内容若不特意说明均为 standalone 模式内容

在介绍[AppClient 向 Master 注册 Application](http://www.jianshu.com/p/5175dfd679b8)的过程中，我们知道 Master 在处理 AppClient 的注册消息时，会进行调度，调度的过程中会决定在某个 worker 上启动某个（或某些） executor，这时会向指定的 worker 发送 ```LaunchExecutor``` 消息，本文将对 worker 接收到该消息后如何启动 executor 进行剖析。

## worker 启动 executor
worker 接收到 ```LaunchExecutor``` 消息后的处理流程如下图所示，主要有四个步骤，我们仅对最关键的创建 ExecutorRunner 对象的创建与启动进行分析


![](http://upload-images.jianshu.io/upload_images/204749-1734c80ed5e3bf9a.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


### ExecutorRunner 对象的创建与启动
ExecutorRunner 是用来管理 executor 进程的，只在 Standalone 模式下有。创建 ExecutorRunner 对象 manager 时，仅对其成员变量做了简单的初始化。关键还是在于 manager 调用的 ```start()``` 方法，该方法实现如下：


![](http://upload-images.jianshu.io/upload_images/204749-a1ef6a54edc20a2f.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


那么上图中在 start() 方法中新创建的线程中调用的 ```ExecutorRunner#fetchAndRunExecutor``` 又做了什么呢？该方法主要做了以下事情：

1. 结合 ApplicationDescription.command 启动 CoarseGrainedExecutorBackend 进程
    1. CoarseGrainedExecutorBackend 在启动后，会向 driver 发送 RegisterExecutor 消息注册 executor
    2. driver 在接收到 RegisterExecutor 消息后，会将 Executor 的信息保存在本地，并响应 ```RegisteredExecutor``` 消息
    3. 回到 CoarseGrainedExecutorBackend，它在接收到 driver 回应的 ```RegisteredExecutor``` 消息后，会创建一个 Executor。至此，Executor 创建完毕(Executor 在 Mesos、YARN、Standalone 模式下都是相同的，不同的只是资源的分配方式)
    4. driver 端调用 CoarseGrainedSchedulerBackend.DriverEndpoint#makeOffers() 实现在 Executor 上启动 task
2. 阻塞等待该 CoarseGrainedExecutorBackend 进程退出
3. 该 CoarseGrainedExecutorBackend 进程退出后，向 worker 发送 ExecutorStateChanged（Executor 状态变更为 EXITED） 消息通知其 Executor 退出

其中，在创建、启动或等待 CoarseGrainedExecutorBackend 进程的过程中:

* 若捕获到 ```InterruptedException``` 类型异常，表明 worker 进程被强制 kill, 则将 Executor 状态置为 KILLED 并调用 killProcess 方法来结束 CoarseGrainedExecutorBackend 进程
* 若捕获到其他类型异常，表明 worker 进程意外退出，则将 Executor 的状态置为 FAILED 并调用 killProcess 方法来结束 CoarseGrainedExecutorBackend 进程

至此，我们完成了对 executor 启动过程的分析。

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
