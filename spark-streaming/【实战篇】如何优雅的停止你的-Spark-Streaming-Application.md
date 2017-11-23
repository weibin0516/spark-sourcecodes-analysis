##Spark 1.3及其前的版本
你的一个 spark streaming application 已经好好运行了一段时间了，这个时候你因为某种原因要停止它。你应该怎么做？直接暴力 kill 该 application 吗?这可能会导致数据丢失，因为 receivers 可能已经接受到了数据，但该数据还未被处理，当你强行停止该 application，driver 就没办法处理这些本该处理的数据。

所以，我们应该使用一种避免数据丢失的方式，官方建议调用 ```StreamingContext#stop(stopSparkContext: Boolean, stopGracefully: Boolean)```，将 stopGracefully 设置为 true，这样可以保证在 driver 结束前处理完所有已经接受的数据。

一个 streaming application 往往是长时间运行的，所以存在两个问题：

1. 应该在什么时候去调用 ```StreamingContext#stop```
2. 当 streaming application 已经在运行了该怎么去调用 ```StreamingContext#stop```

##how
通过 ```Runtime.getRuntime().addShutdownHook``` 注册关闭钩子， JVM将在关闭之前执行关闭钩子中的 ```run``` 函数（不管是正常退出还是异常退出都会调用），所以我们可以在 driver 代码中加入以下代码：

```
Runtime.getRuntime().addShutdownHook(new Thread() {
  override def run() {
    log("Shutting down streaming app...")
    streamingContext.stop(true, true)
    log("Shutdown of streaming app complete.")
  }
})
```

这样就能保证即使 application 被强行 kill 掉，在 driver 结束前，```streamingContext.stop(true, true)```也会被调用，从而保证已接收的数据都会被处理。

##Spark 1.4及其后的版本
上一小节介绍的方法仅适用于 1.3及以前的版本，在 1.4及其后的版本中不仅不能保证生效，甚至会引起死锁等线程问题。在 1.4及其后的版本中，我们只需设置 ```spark.streaming.stopGracefullyOnShutdown``` 为 ```true``` 即可达到上一小节相同的效果。

下面来分析为什么上一小节介绍的方法在 1.4其后的版本中不能用。首先，需要明确的是：

1. 当我们注册了多个关闭钩子时，JVM开始启用其关闭序列时，它会以某种未指定的顺序启动所有已注册的关闭钩子，并让它们同时运行
2. 万一不止一个关闭钩子，它们将并行地运行，并容易引发线程问题，例如死锁

综合以上两点，我们可以明确，如果除了我们注册的关闭钩子外，driver 还有注册了其他钩子，将会引发上述两个问题。

在 StreamingContext#start 中，会调用

```
ShutdownHookManager.addShutdownHook(StreamingContext.SHUTDOWN_HOOK_PRIORITY)(stopOnShutdown)
```

该函数最终注册一个关闭钩子，并会在 ```run``` 方法中调用 ```stopOnShutdown```，

```
  private def stopOnShutdown(): Unit = {
    val stopGracefully = conf.getBoolean("spark.streaming.stopGracefullyOnShutdown", false)
    logInfo(s"Invoking stop(stopGracefully=$stopGracefully) from shutdown hook")
    // Do not stop SparkContext, let its own shutdown hook stop it
    stop(stopSparkContext = false, stopGracefully = stopGracefully)
  }
```

从 ```stopOnShutdown``` 中会根据 ```stopGracefully``` 的值来决定是否以优雅的方式结束 ```driver，而 stopGracefully``` 的值由 ```spark.streaming.stopGracefullyOnShutdown``` 决定。结合上文，也就能说明为什么 ```spark.streaming.stopGracefullyOnShutdown```能决定是否优雅的结束 application 和为什么上一小节的方法不适用与 1.4及其后版本。

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
