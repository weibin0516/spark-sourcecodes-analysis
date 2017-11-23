> 本文基于 Spark 2.1，其他版本实现可能会有所不同

之前写过不少 Spark Core、Spark Streaming 相关的文章，但使用更广泛的 Spark Sql 倒是极少，恰好最近工作中使用到了，便开始研读相关的源码以及写相应的文章，这篇便作为 Spark Sql 系列文章的第一篇。

既然是第一篇，那么就来说说在 Spark Sql 中一条 sql 语句的主要执行流程，来看看下面这个简单的例子：

```
val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()

// For implicit conversions like converting RDDs to DataFrames
import spark.implicits._

val df = spark.read.json("examples/src/main/resources/people.json")

// Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("people")

val sqlDF = spark.sql("SELECT * FROM people")
sqlDF.show()
// +----+-------+
// | age|   name|
// +----+-------+
// |null|Michael|
// |  30|   Andy|
// |  19| Justin|
// +----+-------+
```

上面这段代码主要做了这么几件事：

1. 读取 json 文件得到 df
2. 基于 df 创建临时视图 people
3. 执行 sql 查询 ```SELECT * FROM people```，得到 sqlDF
4. 打印出 sqlDF 的前 20 条记录

在这里，主要关注第 3、4 步。第3步是从 sql 语句转化为 DataFrame 的过程，该过程尚未执行 action 操作，并没有执行计算任务；第4步是一个 action 操作，会触发计算任务的调度、执行。下面，我们分别来看看这两大块

## sql 语句到 sqlDataFrame
这个过程的 uml 时序图如下：


![](http://upload-images.jianshu.io/upload_images/204749-7077714cd2be7f12.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)



根据该时序图，我们对该过程进一步细分：

* 第1~3步：将 sql 语句解析为 unresolved logical plan，可以大致认为是解析 sql 为抽象语法树
* 第4~13步：使用之前得到的 unresolved logical plan 来构造 QueryExecution 对象 qe，qe 与 Row 编码器一起来构造 DataFrame（QueryExecution 是一个关键类，之后的 logical plan 的 analyzer、optimize 以及将 logical plan 转换为 physical plan 都要通过这个类的对象 qe 来调用）

需要注意的是，到这里为止，虽然 ```SparkSession#sql``` 已经返回，并生成了 sqlDataFrame，但由于该 sqlDataFrame 并没有执行任何 action 操作，所以到这里为止，除了在 driver 端执行了上述分析的操作外，其实并没有触发或执行其他的计算任务。

这个过程最重要的产物 unresolved logical plan 被存放在 ```sqlDataFrame.queryExecution``` 中，即 ```sqlDataFrame.queryExecution.logical```

## sqlDataFrame 的 action
前面已经得到了 unresolved logical plan 以及 sqlDataFrame，这里便要执行 action 操作来触发并执行计算任务，大致流程如下：


![](http://upload-images.jianshu.io/upload_images/204749-ea822e91bcc4f173.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)



同样可以将上面这个过程进行细分（忽略第1、2步）：

1. 第3~5步：从更外层慢慢往更直接的执行层的一步步调用
2. 第6步：Analyzer 借助于数据元数据（Catalog）将 unresolved logical plan 转化为 resolved logical plan
3. 第7~8步：Optimizer 将包含的各种优化规则作用于 resolved plan 进行优化
4. 第9~10步：SparkPlanner 将 optimized logical plan 转换为 physical plan
5. 第11~12步：调用 ```QueryExecution#prepareForExecution``` 方法，将 physical plan 转化为 executable physical plan，主要是插入 shuffle 操作和 internal row 的格式转换
6. 第13~14步：将 executable physical plan 转化为 RDD，并调用 RDD collect 来触发计算

## 总结

如果将 sql 到 dataFrame 及 dataFrame action 串起来，简化上文的分析，最核心的流程应该如下图所示：

![](http://upload-images.jianshu.io/upload_images/204749-8a58b60c7bbcf443.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


这篇文章是一片相对宏观的整体流程的分析，目的有二：

* 一是说清楚 Spark Sql 中一条 sql 语句的执行会经过哪几个核心的流程，各个核心流程大概做了什么
* 二是这里指出的各个核心流程也是接下来进一步进行分析学习的方向

更多关于各个流程的进一步实现分析请见之后的文章

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
