Spark有个关于是否允许一个application存在多个SparkContext实例的配置项, 如下:

**spark.driver.allowMultipleContexts: ** If true, log warnings instead of throwing exceptions when multiple SparkContexts are active. 

该值默认为false, 即不允许一个application同时存在一个以上的avtive SparkContext实例. 如何保证这一点呢?

在SparkContext构造函数最开始处获取是否允许存在多个SparkContext实例的标识allowMultipleContexts, 我们这里只讨论否的情况 ( 默认也是否, 即allowMultipleContexts为false )

```
class SparkContext(config: SparkConf) extends Logging with ExecutorAllocationClient {

  //< 如果为true,有多个SparkContext处于active状态时记录warning日志而不是抛出异常.
  private val allowMultipleContexts: Boolean =
    config.getBoolean("spark.driver.allowMultipleContexts", false)
    
    //< 此处省略n行代码
}
```

```

  //< 注意: 这必须放在SparkContext构造器的最开始
  SparkContext.markPartiallyConstructed(this, allowMultipleContexts)
  
  private[spark] def markPartiallyConstructed(
      sc: SparkContext,
      allowMultipleContexts: Boolean): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      assertNoOtherContextIsRunning(sc, allowMultipleContexts)
      contextBeingConstructed = Some(sc)
    }
  }
  ```
  
  ```
  //< 伴生对象SparkContext包含一组实用的转换和参数来和各种Spark特性一起使用
object SparkContext extends Logging {
  private val SPARK_CONTEXT_CONSTRUCTOR_LOCK = new Object()
  
  //< 此处省略n行代码
}
```

结合以上三段代码, 可以看出保证一个Spark Application只有一个SparkContext实例的步骤如下:

1. 通过SparkContext伴生对象object SparkContext中维护了一个对象 ```SPARK_CONTEXT_CONSTRUCTOR_LOCK```, 单例SparkContext在一个进程中是唯一的, 所以```SPARK_CONTEXT_CONSTRUCTOR_LOCK```在一个进程中也是唯一的
2. 函数markPartiallyConstructed中通过synchronized方法保证同一时间只有一个线程能处理

	```
      assertNoOtherContextIsRunning(sc, allowMultipleContexts)
      contextBeingConstructed = Some(sc)
	```
assertNoOtherContextIsRunning会检测是否有其他SparkContext对象正在被构造或已经构造完成, 若allowMultipleContexts为true且确有正在或者已经完成构造的SparkContext对象, 则抛出异常, 否则完成SparkContext对象构造

看到这里, 有人可能会有疑问, 这虽然能保证在一个进程内只有唯一的SparkContext对象, 但Spark是分布式的, 是不是无法保证在在其他节点的进程内会构造SparkContext对象. 其实并不存在这样的问题, 因为SparkContext只会在Driver中得main函数中声明并初始化, 也就是说只会在Driver所在节点的一个进程内构造.

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
