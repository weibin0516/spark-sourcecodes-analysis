> 欢迎关注我的微信公众号：FunnyBigData

## 概述
Structured Streaming 是一个**基于 Spark SQL 引擎**的、可扩展的且支持容错的流处理引擎。你可以像表达静态数据上的批处理计算一样表达流计算。Spark SQL 引擎将随着流式数据的持续到达而持续运行，并不断更新结果。你可以在Scala，Java，Python或R中使用 Dataset/DataFrame API 来表示流聚合，事件时间窗口（event-time windows），流到批处理连接（stream-to-batch joins）等。计算在相同的优化的 Spark SQL 引擎上执行。最后，**通过 checkpoint 和 WAL，系统确保端到端的 exactly-once**。简而言之，Structured Streaming 提供了快速、可扩展的、容错的、端到端 exactly-once 的流处理。

在本指南中，我们将引导你熟悉编程模型和 API。首先，我们从一个简单的例子开始：streaming word count。

## 快速示例
假设要监听从本机 9999 端口发送的文本的 WordCount，让我们看看如何使用结构化流式表达这一点。 首先，必须 import 必须的类并创建 SparkSession

```
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder
  .appName("StructuredNetworkWordCount")
  .getOrCreate()
  
import spark.implicits._
```

然后，创建一个流式 Streaming DataFrame 来代表不断从 ```localhost:9999``` 接收数据，并在该 DataFrame 上执行 transform 来计算 word counts。

```
// Create DataFrame representing the stream of input lines from connection to localhost:9999
val lines = spark.readStream
  .format("socket")
  .option("host", "localhost")
  .option("port", 9999)
  .load()

// Split the lines into words
val words = lines.as[String].flatMap(_.split(" "))

// Generate running word count
val wordCounts = words.groupBy("value").count()
```

DataFrame ```lines``` **代表一个包含流数据的无限的表**。该表包含一个 string 类型的 value 列，**流数据里的每条数据变成了该表中的一行**。接下来，我们调用 ```.as[String]``` 将 DataFrame 转化为 Dataset，这样我们就可以执行 ```flatMap``` 来 split 一行为多个 words。返回值 Dataset ```words``` 包含所有的 words。最后，执行 ```words.groupBy("value").count()``` 得到 ```wordCounts```，**注意，这是一个流式的 DataFrame，代表这个流持续运行中的 word counts**。

现在我们设置好了要在流式数据上执行的查询，接下来要做的就是真正启动数据接收和计算。要做到这一点，我们设置了每当结果有更新就输出完整的结果（通过 ```outputMode("complete")```指定）至控制台。然后调用 ```start``` 来启动流计算。

```
// Start running the query that prints the running counts to the console
val query = wordCounts.writeStream
  .outputMode("complete")
  .format("console")
  .start()

query.awaitTermination()
```

当上面的代码运行起来后，流式计算会在后台启动，```.awaitTermination()``` 会一直等待到计算结束。

另外，需要执行 Netcat 来向 ```localhost:9999``` 发送数据，比如：

```
$ nc -lk 9999
apache spark
apache hadoop
...
```

然后，计算再接收到数据后会不断打印出结果：

```
# TERMINAL 2: RUNNING StructuredNetworkWordCount

-------------------------------------------
Batch: 0
-------------------------------------------
+------+-----+
| value|count|
+------+-----+
|apache|    1|
| spark|    1|
+------+-----+

-------------------------------------------
Batch: 1
-------------------------------------------
+------+-----+
| value|count|
+------+-----+
|apache|    2|
| spark|    1|
|hadoop|    1|
+------+-----+
...
```

## 编程模型
Structured Streaming 的关键思想是**将持续不断的数据当做一个不断追加的表**。这使得流式计算模型与批处理计算引擎十分相似。你将使用类似对于静态表的批处理方式来表达流计算，然后 Spark 以在无限表上的增量计算来运行。

### 基本概念
**将输入的流数据当做一张 “输入表”。把每一条到达的数据作为输入表的新的一行来追加**。


![](http://upload-images.jianshu.io/upload_images/204749-b5891e74db6b8ab5.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


在输入表上执行的查询将会生成 “结果表”。每个触发间隔（trigger interval）（例如 1s），新的行追加到输入表，最终更新结果表。无论何时更新结果表，我们都希望将更改的结果行 output 到外部存储/接收器（external sink）。


![](http://upload-images.jianshu.io/upload_images/204749-e0a81f1829cf5e45.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


output 有以下三种模式：

* Complete Mode：**整个更新的结果表将被写入外部存储**。由存储连接器（storage connector）决定如何处理整个表的写入
* Append Mode：**只有结果表中自上次触发后附加的新行将被写入外部存储。这仅适用于不期望更改结果表中现有行的查询**。
* Update Mode：**只有自上次触发后结果表中更新的行将被写入外部存储**（自 Spark 2.1.1 起可用）。 请注意，这与完全模式不同，因为此模式仅输出自上次触发以来更改的行。**如果查询不包含聚合操作，它将等同于附加模式**。

请注意，每种模式适用于某些类型的查询。这将在后面详细讨论。

为了说明这个模型的使用，让我们来进一步理解上面的快速示例：

* 最开始的 DataFrame ```lines``` 为输入表
* 最后的 DataFrame ```wordCounts``` 为结果表

在流上执行的查询将 DataFrame ```lines``` 转化为 DataFrame ```wordCounts``` 与在静态 DataFrame 上执行的操作完全相同。当启动计算后，Spark 会不断从 socket 连接接收数据。如果有新的数据到达，Spark将运行一个 “增量” 查询，将以前的 counts 与新数据相结合，以计算更新的 counts，如下所示：


![](http://upload-images.jianshu.io/upload_images/204749-a5681561440934b6.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


这种模式与许多其他流处理引擎有显著差异。许多流处理引擎要求用户自己维护运行的状态，因此必须对容错和数据一致性（at-least-once, or at-most-once, or exactly-once）进行处理。 在这个模型中，当有新数据时，Spark负责更新结果表，从而减轻用户的工作。作为例子，我们来看看该模型如何处理 event-time 和延迟的数据。

### 处理 event-time 和延迟数据
event-time 是嵌入在数据中的时间。对于许多 application，你可能希望在 event-time 上进行操作。例如，如果要每分钟获取IoT设备生成的事件数，则会希望使用数据生成的时间（即嵌入在数据中的 event-time），而不是 Spark 接收到数据的时间。在该模型中 event-time 被非常自然的表达，来自设备的每个事件都是表中的一行，**event-time 是行中的一列**。这允许基于 window 的聚合（例如每分钟的事件数）仅仅是 event-time 列上的特殊类型的分组（grouping）和聚合（aggregation）：每个时间窗口是一个组，并且每一行可以属于多个窗口/组。因此，可以在静态数据集和数据流上进行基于事件时间窗口（ event-time-window-based）的聚合查询，从而使用户操作更加方便。

此外，该模型也可以自然的处理接收到的时间晚于 event-time 的数据。因为 Spark 一直在更新结果表，所以它可以完全控制更新旧的聚合数据，或清除旧的聚合以限制中间状态数据的大小。**自 Spark 2.1 起，开始支持 watermark 来允许用于指定数据的超时时间（即接收时间比 event-time 晚多少），并允许引擎相应的清理旧状态**。这将在下文的 “窗口操作” 小节中进一步说明。

### 容错语义
提供端到端的 exactly-once 语义是 Struectured Streaming 背后设计的关键目标之一。为了达到这点，设计了 Structured Streaming 的 sources（数据源）、sink（输出）以及执行引擎可靠的追踪确切的执行进度以便于通过重启或重新处理来处理任何类型的故障。对于每个具有偏移量（类似于 Kafka 偏移量或 Kinesis 序列号）的 streaming source。引擎使用 checkpoint 和 WAL 来记录每个 trigger 处理的 offset 范围。**streaming sinks 被设计为对重新处理是幂等的**。结合可以重放的 sources 和支持重复处理幂等的 sinks，不管发生什么故障 Structured Streaming 可以确保端到端的 exactly-once 语义。

## 使用 Datasets 和 DataFrames API
**自 Spark 2.0 起，Spark 可以代表静态的、有限数据和流式的、无限数据**。与静态的 Datasets/DataFrames 类似， 你可以使用 SparkSession 基于 streaming sources 来创建 DataFrames/Datasets，并且与静态 DataFrames/Datasets 使用相同的操作。

### 创建流式 DataFrames 和流式 Datasets
流式 DataFrames 可以通过 DataStreamReader 创建，DataStreamReader 通过调用 ```SparkSession.readStream()``` 创建。与静态的 ```read()``` 方法类似，你可以指定 source 的详细信息：格式、schema、选项等。

#### 输入源
在 Spark 2.0 中，只有几个内置的 sources：

* File source：以文件流的形式读取目录中写入的文件。支持的文件格式为text，csv，json，parquet。请注意，**文件必须以原子方式放置在给定的目录中，这在大多数文件系统中可以通过文件移动操作实现**。
* Kafka source：从 Kafka 拉取数据。兼容 Kafka 0.10.0 以及更高版本。
* Socket source（仅做测试用）：从 socket 读取 UTF-8 文本数据。请注意，这只能用于测试，因为它不提供端到端的容错

某些 source 不是容错的，因为它们不能保证在故障后可以重放数据。以下是 Spark 中所有 sources 的详细信息：

* File Source：
    * options：
        * path：输入目录的路径，所有格式通用
        * maxFilesPerTrigger：**每次 trigger 最大文件数（默认无限大）**
        * latestFirst：**是否首先处理最新的文件，当有大量积压的文件时很有用（默认 false）**
        * fileNameOnly：是否仅根据文件名而不是完整路径检查新文件（默认 false）。将此设置为“true”，以下文件将被视为相同的文件，因为它们的文件名“dataset.txt”是相同的：```"file:///dataset.txt"、"s3://a/dataset.txt"、"s3n://a/b/dataset.txt"、"s3a://a/b/c/dataset.txt"```
    * 容错：支持
    * 注意：**支持通配符路径，但不支持逗号分隔的多个路径/通配符路径**
* Socket Source：
    * options：
        * host: 要连接的 host, 必须指定
        * port: 要连接的 port, 必须指定
    * 容错：不支持
    * 注意：无
* Kafka Source：
    * options：详见[Kafka Integration Guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
    * 容错：支持
    * 注意：无

以下是一些例子：

```
val spark: SparkSession = ...

// Read text from socket
val socketDF = spark
  .readStream
  .format("socket")
  .option("host", "localhost")
  .option("port", 9999)
  .load()

socketDF.isStreaming    // Returns True for DataFrames that have streaming sources

socketDF.printSchema

// Read all the csv files written atomically in a directory
val userSchema = new StructType().add("name", "string").add("age", "integer")
val csvDF = spark
  .readStream
  .option("sep", ";")
  .schema(userSchema)      // Specify schema of the csv files
  .csv("/path/to/directory")    // Equivalent to format("csv").load("/path/to/directory")
```

这些示例生成的流 DataFrames 是无类型的，在编译时并不会进行类型检查，只在运行时进行检查。某些操作，比如 map、flatMap 等，需要在编译时就知道类型，这时你可以将 DataFrame 转换为 Dataset（使用与静态相同的方法）。

### 流式 DataFrames/Datasets 的 schema 推断和分区
默认情况下，基于 File Source 需要你自行指定 schema，而不是依靠 Spark 自动推断。这样的限制确保了 streaming query 会使用确切的 schema。你也可以通过将```spark.sql.streaming.schemaInference``` 设置为 true 来重新启用 schema 推断。

**当子目录名为 ```/key=value/``` 时，会自动发现分区，并且对这些子目录进行递归发现**。如果这些列出现在提供的 schema 中，spark 会读取相应目录的文件并填充这些列。可以增加组成分区的目录，比如当 ```/data/year=2015/``` 存在是可以增加 ```/data/year=2016/```；但修改分区目录是无效的，比如创建目录 ```/data/date=2016-04-17/```。

### 流式 DataFrames/Datasets 上的操作
你可以在流式 DataFrames/Datasets 上应用各种操作：从无类型，类似 SQL 的操作（比如 select、where、groupBy），到类似有类型的 RDD 操作（比如 map、filter、flatMap）。让我们通过几个例子来看看。

#### 基本操作 - Selection, Projection, Aggregation
大部分常见的 DataFrame/Dataset 操作也支持流式的 DataFrame/Dataset。少数不支持的操作将会在后面进行讨论。

```
case class DeviceData(device: String, deviceType: String, signal: Double, time: DateTime)

val df: DataFrame = ... // streaming DataFrame with IOT device data with schema { device: string, deviceType: string, signal: double, time: string }
val ds: Dataset[DeviceData] = df.as[DeviceData]    // streaming Dataset with IOT device data

// Select the devices which have signal more than 10
df.select("device").where("signal > 10")      // using untyped APIs   
ds.filter(_.signal > 10).map(_.device)         // using typed APIs

// Running count of the number of updates for each device type
df.groupBy("deviceType").count()                          // using untyped API

// Running average signal for each device type
import org.apache.spark.sql.expressions.scalalang.typed
ds.groupByKey(_.deviceType).agg(typed.avg(_.signal))    // using typed API
```

#### event-time（事件时间）上的 window 操作
使用 Structured Streaming 进行滑动的 event-time 窗口聚合是很简单的，与分组聚合非常类似。在分组聚合中，为用户指定的分组列中的每个唯一值维护一个聚合值（例如计数）。**在基于 window 的聚合的情况下，为每个 window 维护聚合（aggregate values），流式追加的行根据 event-time 落入相应的聚合**。让我们通过下图来理解。

想象下，我们的快速示例现在改成了包含数据生成的时间。现在我们想在 10 分钟的 window 内计算 word count，每 5 分钟更新一次。比如 ```12:00 - 12:10, 12:05 - 12:15, 12:10 - 12:20``` 等。```12:00 - 12:10``` 是指数据在 12:00 之后 12:10 之前到达。现在，考虑一个 word 在 12:07 的时候接收到。该 word 应当增加 ```12:00 - 12:10``` 和 ```12:05 - 12:15``` 相应的 counts。所以 counts 会被分组的 key 和 window 分组。

结果表将如下所示：


![](http://upload-images.jianshu.io/upload_images/204749-6dee56f18a9abbdd.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


由于这里的 window 与 group 非常类似，在代码上，你可以使用 ```groupBy``` 和 ```window``` 来表达 window 聚合。例子如下：

```
import spark.implicits._

val words = ... // streaming DataFrame of schema { timestamp: Timestamp, word: String }

// Group the data by window and word and compute the count of each group
val windowedCounts = words.groupBy(
  window($"timestamp", "10 minutes", "5 minutes"),
  $"word"
).count()
```

#### Watermark 和延迟数据处理
现在考虑一个数据延迟到达会怎么样。例如，一个在 12:04 生成的 word 在 12:11 被接收到。**application 会使用 12:04 而不是 12:11 去更新 ```12:00 - 12:10```的 counts**。这在基于 window 的分组中很常见。Structured Streaming 会长时间维持部分聚合的中间状态，以便于后期数据可以正确更新旧 window 的聚合，如下所示：


![](http://upload-images.jianshu.io/upload_images/204749-f957e3c97e23514c.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


然后，当 query 运行了好几天，系统必须限制其累积的内存中中间状态的数量。这意味着系统需要知道什么时候可以从内存状态中删除旧的聚合，因为 application 不会再为该聚合更晚的数据进行聚合操作。为启动此功能，在Spark 2.1中，引入了 watermark（水印），使引擎自动跟踪数据中的当前事件时间，并相应地清理旧状态。你可以通过指定事件时间列来定义一个 query 的 watermark 和 late threshold（延迟时间阈值）。**对于一个开始于 T 的 window，引擎会保持中间状态并允许后期的数据对该状态进行更新直到 ```max event time seen by the engine - late threshold > T```。换句话说，在延迟时间阈值范围内的延迟数据会被聚合，但超过该阈值的数据会被丢弃**。让我们以一个例子来理解这一点。我们可以使用 ```withWatermark()``` 定义一个 watermark，如下所示：

```
import spark.implicits._

val words = ... // streaming DataFrame of schema { timestamp: Timestamp, word: String }

// Group the data by window and word and compute the count of each group
val windowedCounts = words
    .withWatermark("timestamp", "10 minutes")
    .groupBy(
        window($"timestamp", "10 minutes", "5 minutes"),
        $"word")
    .count()
```

在这个例子中，我们定义了基于 timestamp 列定义了 watermark，并且将 10 分钟定义为允许数据延迟的阈值。如果该数据以 update 输出模式运行：

* 引擎将不断更新结果表中 window 中的 counts 直到该 window 比 watermark 更旧
* 数据中的 timestamp 值比当前的最大 event-time 落后 10 分钟以上的数据将被丢弃

以下为示图：


![](http://upload-images.jianshu.io/upload_images/204749-c032a45ae78b2c94.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


如图所示，引擎跟踪的最大 event-time 是蓝色虚线，并且在每个 trigger 开始时设置 watermark 为 ``` (max event time - '10 mins') ``` 的红线例如，当引擎发现 ```(12:14, dog)``` 时将下次 trigger 的 watermark 设置为 12:04。然后，当 watermark 更新为 12:11 时，window ```（12:00 - 12:10）``` 的中间状态被清除，所有后续数据（例如```（12:04，donkey）```）被认为是“太晚”，因此被丢弃。根据 output 模式，每次触发后，更新的计数（即紫色行）都将作为触发输出进行写入到 sink。

某些 sink（例如文件）可能不支持 update mode 所需的细粒度更新。所以，我们还支持 append 模式，只有最后确定的计数被写入。这如下图所示。

注意，在非流式 Dataset 上使用 withWatermark 是无效的空操作。


![](http://upload-images.jianshu.io/upload_images/204749-96ede5e2a61f78d6.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


与之前的 update mode 类似，引擎维护每个 window 的中间计数。**只有当 ```window < watermark``` 时才会删除 window 的中间状态数据，并将该 window 最终的 counts 追加到结果表或 sink 中**。例如，window ```12:00 - 12:10``` 的最终结果将在 watermark 更新到 12:11 后再追加到结果表中。

watermark 清除聚合状态的条件十分重要，为了清理聚合状态，必须满足以下条件（自 Spark 2.1.1 起，将来可能会有变化）：

* **output mode 必须为 append 或 update：complete mode 需要保留所有的聚合数据，因此 watermark 不能用来清理聚合数据**
* **聚合必须具有 event-time 列或基于 event-time 的 window**
* **withWatermark 必须调用在用来聚合的时间列上**。比如 ```df.withWatermark("time", "1 min").groupBy("time2").count()``` 是无效的
* **withWatermark 必须在调用聚合前调用来说明 watermark 的细**节。比如，```df.groupBy("time").count().withWatermark("time", "1 min")``` 是无效的

#### Join 操作
**流式 DataFrames 可以与静态 DataFrames 进行 join 来创建新的流式 DataFrames**。如下：

```
val staticDf = spark.read. ...
val streamingDf = spark.readStream. ...

streamingDf.join(staticDf, "type")          // inner equi-join with a static DF
streamingDf.join(staticDf, "type", "right_join")  // right outer join with a static DF
```

#### 流重复数据的删除（去重）
你可以使用事件中的唯一标识符对数据流中的记录进行重复数据删除。这与使用唯一标识符列的静态重复数据消除完全相同。**该查询会存储所需的一定量先前的数据，以便可以过滤重复的记录**。类似于聚合，你可以使用或不使用 watermark 来删除重复数据，如下例子：

* 使用 watermark：如果重复记录可能到达的时间有上限，则可以在事件时间列上定义 watermark，并使用 guid 和事件时间列进行重复数据删除
* 不使用 watermark：由于重复记录可能到达的时间没有上限，会将来自过去所有记录的数据存储为状态

```
val streamingDf = spark.readStream. ...  // columns: guid, eventTime, ...

// Without watermark using guid column
streamingDf.dropDuplicates("guid")

// With watermark using guid and eventTime columns
streamingDf
  .withWatermark("eventTime", "10 seconds")
  .dropDuplicates("guid", "eventTime")
```

#### 任意有状态的操作
许多场景需要使用比聚合更复杂的状态操作，可能不得不把任意类型的数据保存为状态，并使用每个 trigger 中的流式事件对状态执行任意操作。自 Spark2.2 起，这可以通过调用 ```mapGroupWithState``` 和 ```flatMapGroupWithState``` 做到。**这两个操作都允许你在分组的数据集上应用用户定义的代码来更新用户定义的状态**，有关更具体的细节，请查看API文档 [GroupState](https://spark.apache.org/docs/2.2.0/api/scala/index.html#org.apache.spark.sql.streaming.GroupState) 和 [example](https://github.com/apache/spark/blob/v2.2.0/examples/src/main/scala/org/apache/spark/examples/sql/streaming/StructuredSessionization.scala)。

#### 不支持的操作
DataFrame/Dataset 有一些操作是流式 DataFrame/Dataset 不支持的，其中的一些如下：

* 不支持多个流聚合
* 不支持 limit、first、take 这些取 N 条 Row 的操作
* 不支持 Distinct
* 只有当 output mode 为 complete 时才支持排序操作
* 有条件地支持流和静态数据集之间的外连接：
    * 不支持与流式 Dataset 的全外连接（full outer join）
    * 不支持左侧外连接（left outer join）与右侧的流式 Dataset
    * 右侧外连接与左侧的流式 Dataset 不支持

此外，还有一些 Dataset 方法将不适用于流数据集。它们是立即运行查询并返回结果的操作，这在流数据集上没有意义。相反，这些功能可以通过显式启动流式查询来完成。

* ```count()```：无法从流式 Dataset 返回单个计数。而是使用 ```ds.groupBy().count()``` 返回一个包含运行计数的 streaming Dataset
* ```foreach()```：使用 ```ds.writeStream.foreach(...)``` 代替
* ```show()```：使用输出到 console sink 代替

如果你执行了这些操作，你会看到一个 AnalysisException，像 ```operation XYZ is not supported with streaming DataFrames/Datasets”```。虽然其中一些可能在未来版本的 Spark 中得到支持，还有其他一些从根本上难以有效地实现。例如，不支持对输入流进行排序，因为它需要跟踪流中接收到的所有数据，这从根本上是很难做到的。

## 启动流式查询
一旦定义了最终的结果 ```DataFrame/Dataset```，剩下的就要启动流计算。要做到这一点，必须使用通过调用 ```Dataset.writeStream()``` 返回的 DataStreamWriter。必须指定以下的一个或多个：

* output sink 细节：data format、location 等
* output mode
* query name：可选的，指定用于识别的查询的唯一名称
* trigger interval：可选的，**如果没有指定，则系统将在上一次处理完成后立即检查是否有新的可用数据。如果由于上一次的触发还未完成导致下一次的触发时间错过了，系统会在下一次的触发时间进行触发而不是在上一次触发结束后立马触发**
* checkpoint location：对于那些可以保证端到端容错的 output sinks，系统会往指定的 location 写入所有的 checkpoint 信息。该 location 必须是一个 HDFS 兼容的文件系统。checkpoint 会在下一节中进行更详细得介绍

### Output Modes
有几种类型的输出模式：

* Append mode（默认的）：这是默认模式，其中只有从上次触发后添加到结果表的新行将被输出到 sink。适用于那些添加到结果表中的行从不会更改的查询。只有 select、where、map、flatMap、filter、join 等查询会支持 Append mode
* Complete mode：每次 trigger 后，整个结果表将被输出到 sink。聚合查询（aggregation queries）支持该模式
* Update mode：（自 Spark 2.1.1 可用）。只有结果表中自上次 trigger 后更新的行将被输出到 sink

不同类型的流式 query 支持不同的 output mode。以下是兼容性：


![](http://upload-images.jianshu.io/upload_images/204749-f10abc904bb4bc3f.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


### 输出接收器（Output sink）
有几种类型的内置输出接收器。

* File sink：存储输出至目录：

```
writeStream
    .format("parquet")        // can be "orc", "json", "csv", etc.
    .option("path", "path/to/destination/dir")
    .start()
```

* Foreach sink：对输出中的记录运行任意计算：

```
writeStream
    .foreach(...)
    .start()
```

* Console sink（用来调试）：每次 trigger 将输出打印到控制台。支持 Append 和 Complete 模式。**仅适用于小数据量的调试之用，因为在每次 trigger 之后，完整的输出会被存储在 driver 的内存中**，请谨慎使用：

```
writeStream
    .format("console")
    .start()
```

* Memory sink（用来调试）：输出作为内存表存储在内存中。支持 Append 和 Complete 模式。**仅适用于小数据量的调试之用，因为在每次 trigger 之后，完整的输出会被存储在 driver 的内存中**，请谨慎使用：

```
writeStream
    .format("memory")
    .queryName("tableName")
    .start()
```

某些接收器不容错，因为它们不保证输出的持久性，仅用于调试目的。请参阅上一节关于容错语义的部分。以下是 Spark 中所有内置接收器的详细信息：

![](http://upload-images.jianshu.io/upload_images/204749-5affa545cde7b5b0.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


请注意，必须调用 ```start()``` 来实际启动查询的执行。这将返回一个 StreamingQuery 对象，它是持续运行的查询的句柄。你可以使用该对象来管理查询，我们将在下一小节中讨论。现在，让我们通过几个例子来了解：

```
// ========== DF with no aggregations ==========
val noAggDF = deviceDataDf.select("device").where("signal > 10")   

// Print new data to console
noAggDF
  .writeStream
  .format("console")
  .start()

// Write new data to Parquet files
noAggDF
  .writeStream
  .format("parquet")
  .option("checkpointLocation", "path/to/checkpoint/dir")
  .option("path", "path/to/destination/dir")
  .start()

// ========== DF with aggregation ==========
val aggDF = df.groupBy("device").count()

// Print updated aggregations to console
aggDF
  .writeStream
  .outputMode("complete")
  .format("console")
  .start()

// Have all the aggregates in an in-memory table
aggDF
  .writeStream
  .queryName("aggregates")    // this query name will be the table name
  .outputMode("complete")
  .format("memory")
  .start()

spark.sql("select * from aggregates").show()   // interactively query in-memory table
```

### 使用 Foreach
foreach 操作允许在输出数据上进行任意操作。在 Spark 2.1 中，只有 Scala 和 Java 可用。要使用这个，你**必须实现 ForeachWriter 接口**，其具有每次 trigger 后每当有一系列行生成时会调用的方法，注意一下几个要点：

* writer 必须是可序列化的，因为它将被序列化并发送给 executor 执行
* open、process 和 close 会在 executors 上被调用
* 只有当 open 方法被调用时 writer 才执行所有的初始化。请注意，如果在创建对象时立即进行任何初始化，那么该初始化将在 driver 中发生，这可能不是你预期的
* open 方法可以使用 version 和 partition 来决定是否需要写入序列的行。可以返回 true（继续写入）或 false（无需写入）。如果返回 false，process 不会在任何行上被调用。例如，在部分失败之后，失败的 trigger 的部分输出分区可能已经被提交到数据库。基于存储在数据库中的元数据，可以识别已经提交的分区，因此返回 false 以避免再次提交它们。
* 每当 open 被调用，close 也会被调用（除非 JVM 因为意外退出）。即使 open 返回 false 也是如此。如果在处理和写入数据的时候发生错误，close 会被调用。你有责任清理在 open 中创建的状态（例如连接，事务等），以免资源泄漏

## 管理流式查询
当 query 启动时，StreamingQuery 被创建，可以用来监控和管理该 query：

```
val query = df.writeStream.format("console").start()   // get the query object

query.id          // get the unique identifier of the running query that persists across restarts from checkpoint data

query.runId       // get the unique id of this run of the query, which will be generated at every start/restart

query.name        // get the name of the auto-generated or user-specified name

query.explain()   // print detailed explanations of the query

query.stop()      // stop the query

query.awaitTermination()   // block until query is terminated, with stop() or with error

query.exception       // the exception if the query has been terminated with error

query.recentProgress  // an array of the most recent progress updates for this query

query.lastProgress    // the most recent progress update of this streaming query
```

可以在单个 SparkSession 中启动任意数量的 query。他们都将同时运行共享集群资源。可以调用 ```sparkSession.streams()``` 来获取 StreamingQueryManager，可以用来管理当前 active queries：

```
val spark: SparkSession = ...

spark.streams.active    // get the list of currently active streaming queries

spark.streams.get(id)   // get a query object by its unique id

spark.streams.awaitAnyTermination()   // block until any one of them terminates
```

## 监控流式查询
有两种 API 用于监控和调试 active queries：以交互方式和异步方式。

### 交互式 APIs（Interactive APIs）
你可以调用 ```streamingQuery.lastProgress()``` 和 ```streamingQuery.status()``` 来直接获取某个 query 的当前的状态和指标。```lastProgress``` 返回一个 ```StreamingQueryProgress``` 对象。它具有关于流最后一个 trigger 的进度的所有信息，包括处理哪些数据、处理速度、处理延迟等。还有 ```streamingQuery.recentProgress``` 返回最后几个进度的数组。

另外，```streamingQuery.status()``` 返回一个 ```StreamingQueryStatus```。它提供了有关 query 执行的信息，比如是否有 trigger active，是否有数据正在被处理等。

以下是一些例子：

```
val query: StreamingQuery = ...

println(query.lastProgress)

/* Will print something like the following.

{
  "id" : "ce011fdc-8762-4dcb-84eb-a77333e28109",
  "runId" : "88e2ff94-ede0-45a8-b687-6316fbef529a",
  "name" : "MyQuery",
  "timestamp" : "2016-12-14T18:45:24.873Z",
  "numInputRows" : 10,
  "inputRowsPerSecond" : 120.0,
  "processedRowsPerSecond" : 200.0,
  "durationMs" : {
    "triggerExecution" : 3,
    "getOffset" : 2
  },
  "eventTime" : {
    "watermark" : "2016-12-14T18:45:24.873Z"
  },
  "stateOperators" : [ ],
  "sources" : [ {
    "description" : "KafkaSource[Subscribe[topic-0]]",
    "startOffset" : {
      "topic-0" : {
        "2" : 0,
        "4" : 1,
        "1" : 1,
        "3" : 1,
        "0" : 1
      }
    },
    "endOffset" : {
      "topic-0" : {
        "2" : 0,
        "4" : 115,
        "1" : 134,
        "3" : 21,
        "0" : 534
      }
    },
    "numInputRows" : 10,
    "inputRowsPerSecond" : 120.0,
    "processedRowsPerSecond" : 200.0
  } ],
  "sink" : {
    "description" : "MemorySink"
  }
}
*/


println(query.status)

/*  Will print something like the following.
{
  "message" : "Waiting for data to arrive",
  "isDataAvailable" : false,
  "isTriggerActive" : false
}
*/
```

### 异步 API
你还可以通过附加 ```StreamingQueryListener``` 异步监控与 SparkSession 关联的所有查询。**一旦你通过 ```sparkSession.streams.attachListener()``` 附加了自定义的 ```StreamingQueryListener``` 对象，当 query 启动、结束、active 查询有进展时就会被回调**。下面是一个例子：

```
val spark: SparkSession = ...

spark.streams.addListener(new StreamingQueryListener() {
    override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
    }
    override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
    }
    override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        println("Query made progress: " + queryProgress.progress)
    }
})
```

## 使用 checkpoint 从失败中恢复
在失败或主动 shutdown 的情况下，可以恢复之前的查询进度和状态并从该处继续运行。这是依赖 checkpoint 和 WAL（write ahead logs） 来完成的。你可以配置一个 checkpoint 路径，query 会将进度信息（比如每个 trigger 处理的 offset ranger）和运行中的聚合写入到 checkpoint 的位置。checkpoint 的路径必须是一个 HDFS 兼容的文件系统，并且需要在定义 query 的时候设置好，如下：

```
aggDF
  .writeStream
  .outputMode("complete")
  .option("checkpointLocation", "path/to/HDFS/dir")
  .format("memory")
  .start()
```

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
