> 本文为 Spark 2.0 源码分析笔记，由于源码只包含 standalone 模式下完整的 executor 相关代码，所以本文主要针对 standalone 模式下的 executor 模块，文中内容若不特意说明均为 standalone 模式内容

## 创建 task（driver 端）
task 的创建本应该放在[分配 tasks 给 executors](http://www.jianshu.com/p/17a61ff4d65c)一文中进行介绍，但由于创建的过程与分发及之后的反序列化执行关系紧密，我把这一部分内容挪到了本文。

创建 task 是在 ```TaskSetManager#resourceOffer(...)``` 中实现的，更准确的说是创建 ```TaskDescription```，task 及依赖的环境都会被转换成 byte buffer，然后与 ```taskId、taskName、execId``` 等一起构造 ```TaskDescription``` 对象，该对象将在之后被序列化并分发给 executor 去执行，主要流程如下：


![](http://upload-images.jianshu.io/upload_images/204749-f2f18156070f63bf.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


从流程图中可以看出，task 依赖了的文件、jar 包、设置的属性及其本身都会被转换成 byte buffer 

## 分发 task（driver 端）
分发 task 操作是在 driver 端的 ```CoarseGrainedSchedulerBackend#launchTasks(tasks: Seq[Seq[TaskDescription]])``` 中进行，由于上一步已经创建了 TaskDescription 对象，分发这里要做的事就很简单，如下：


![](http://upload-images.jianshu.io/upload_images/204749-8e0b3a99c427fc2b.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


仅仅是序列化了 ```TaskDescription``` 对象并发送 ```LaunchTask``` 消息给 ```CoarseGrainedExecutorBackend```

## worker 接收并处理 LaunchTask 消息
LaunchTask 消息是由 CoarseGrainedExecutorBackend 接收到的，接收到后的处理流程如下：


![](http://upload-images.jianshu.io/upload_images/204749-209b2328e4380d8d.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)



接收到消息后，CoarseGrainedExecutorBackend 会从消息中反序列化出 TaskDescription 对象并交给 Executor 去执行；Executor 利用 TaskDescription 对象创建 TaskRunner 然后提交到自带的线程池中执行。

关于 TaskRunner、线程池以及 task 具体是如何执行的，将会在下一篇文章中详述，本文只关注创建、分发 task 的过程。

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
