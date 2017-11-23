> 本文为 Spark 2.0 源码分析笔记，其他版本可能稍有不同

[Spark Task 的执行流程③ - 执行 task](http://www.jianshu.com/p/8bb456cb7c77)一文中介绍了 task 是如何执行并返回 task 执行结果的，本文将进一步介绍 task 的结果是怎么处理的。

## worker 端的处理
处理 task 的结果是在 ```TaskRunner#run()``` 中进行的，紧接着 task 执行步骤，结果处理的核心流程如下：


![](http://upload-images.jianshu.io/upload_images/204749-e799dff6b2f4e5e8.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


我们进一步展开上图中浅灰色背景步骤，根据 resultSize（序列化后的 task 结果大小） 大小的不同，共有三种情况：

* ```resultSize > spark.driver.maxResultSize 配置值（默认1G）```：直接丢弃，若有必要需要修改 ```spark.driver.maxResultSize``` 的值。此时，serializedResult 为序列化的 IndirectTaskResult 对象，driver 之后通过该对象是获得不到结果的
* ```resultSize > maxDirectResultSize 且 resultSize <= spark.driver.maxResultSize 配置值```：maxDirectResultSize 为配置的 ```spark.rpc.message.maxSize``` 与 ```spark.task.maxDirectResultSize``` 更小的值；这种情况下，会将结果存储到 BlockManager 中。此时，serializedResult 为序列化的 IndirectTaskResult 对象，driver 之后可以通过该对象在 BlockManager 系统中拉取结果
* ```resultSize <= maxDirectResultSize```：serializedResult 直接就是 serializedDirectResult

在拿到 serializedResult 之后，调用 ```CoarseGrainedExecutorBackend#statusUpdate``` 方法，如下：

```
execBackend.statusUpdate(taskId, TaskState.FINISHED, serializedResult)
```

该方法会使用 driverRpcEndpointRef 发送一条包含 serializedResult 的 ```StatusUpdate``` 消息给 driver (更具体说是其中的 CoarseGrainedSchedulerBackend 对象)

## driver 端的处理
driver 端的 CoarseGrainedSchedulerBackend 在收到 worker 端发送的 ```StatusUpdate``` 消息后，会进行一系列的处理，包括调用 TaskScheduler 方法以做通知，主要流程如下：


![](http://upload-images.jianshu.io/upload_images/204749-95cd467a373614b4.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


其中，需要说明的是 Task 的状态只有为 FINISHED 时才成功，其他值（FAILED, KILLED, LOST）均为失败。

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
