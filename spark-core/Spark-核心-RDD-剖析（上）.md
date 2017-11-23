本文将通过描述 Spark RDD 的五大核心要素来描述 RDD，若希望更全面了解 RDD 的知识，请移步 RDD 论文：[RDD：基于内存的集群计算容错抽象](http://shiyanjun.cn/archives/744.html)

Spark 的五大核心要素包括：

* partition
* partitioner
* compute func
* dependency
* preferredLocation

下面一一来介绍

##(一): partition
###partition 个数怎么定
RDD 由若干个 partition 组成，共有三种生成方式：

* 从 Scala 集合中创建，通过调用 ```SparkContext#makeRDD``` 或 ```SparkContext#parallelize```
* 加载外部数据来创建 RDD，例如从 HDFS 文件、mysql 数据库读取数据等
* 由其他 RDD 执行 transform 操作转换而来

那么，在使用上述方法生成 RDD 的时候，会为 RDD 生成多少个 partition 呢？一般来说，加载 Scala 集合或外部数据来创建 RDD 时，是可以指定 partition 个数的，若指定了具体值，那么 partition 的个数就等于该值，比如：

```
val rdd1 = sc.makeRDD( scalaSeqData, 3 )    //< 指定 partition 数为3
val rdd2 = sc.textFile( hdfsFilePath, 10 )  //< 指定 partition 数为10
```

若没有指定具体的 partition 数时的 partition 数为多少呢？

* 对于从 Scala 集合中转换而来的 RDD：默认的 partition 数为 defaultParallelism，该值在不同的部署模式下不同：
    * Local 模式：本机 cpu cores 的数量
    * Mesos 模式：8
    * Yarn：max(2, 所有 executors 的 cpu cores 个数总和)
* 对于从外部数据加载而来的 RDD：默认的 partition 数为 ```min(defaultParallelism, 2)```
* 对于执行转换操作而得到的 RDD：视具体操作而定，如 map 得到的 RDD 的 partition 数与 父 RDD 相同；union 得到的 RDD 的 partition 数为父 RDDs 的 partition 数之和...

---

### partition 的定义
我们常说，partition 是 RDD 的数据单位，代表了一个分区的数据。但这里千万不要搞错了，partition 是逻辑概念，是代表了一个分片的数据，而不是包含或持有一个分片的数据。

真正直接持有数据的是各个 partition 对应的迭代器，要再次注意的是，partition 对应的迭代器访问数据时也不是把整个分区的数据一股脑加载持有，而是像常见的迭代器一样一条条处理。举个例子，我们把 HDFS 上10G 的文件加载到 RDD 做处理时，并不会消耗10G 的空间，如果没有 shuffle 操作（shuffle 操作会持有较多数据在内存），那么这个操作的内存消耗是非常小的，因为在每个 task 中都是一条条处理处理的，在某一时刻只会持有一条数据。这也是初学者常有的理解误区，一定要注意 Spark 是基于内存的计算，但不会傻到什么时候都把所有数据全放到内存。

让我们来看看 Partition 的定义帮助理解：

```
trait Partition extends Serializable {
  def index: Int

  override def hashCode(): Int = index
}
```

在 trait Partition 中仅包含返回其索引的 index 方法。很多具体的 RDD 也会有自己实现的 partition，比如：

**KafkaRDDPartition 提供了获取 partition 所包含的 kafka msg 条数的方法**

```
class KafkaRDDPartition(
  val index: Int,
  val topic: String,
  val partition: Int,
  val fromOffset: Long,
  val untilOffset: Long,
  val host: String,
  val port: Int
) extends Partition {
  /** Number of messages this partition refers to */
  def count(): Long = untilOffset - fromOffset
}
```

**UnionRDD 的 partition 类 UnionPartition 提供了获取依赖的父 partition 及获取优先位置的方法**

```
private[spark] class UnionPartition[T: ClassTag](
    idx: Int,
    @transient private val rdd: RDD[T],
    val parentRddIndex: Int,
    @transient private val parentRddPartitionIndex: Int)
  extends Partition {

  var parentPartition: Partition = rdd.partitions(parentRddPartitionIndex)

  def preferredLocations(): Seq[String] = rdd.preferredLocations(parentPartition)

  override val index: Int = idx
}
```

###partition 与 iterator 方法
RDD 的 ```def iterator(split: Partition, context: TaskContext): Iterator[T]``` 方法用来获取 split 指定的 Partition 对应的数据的迭代器，有了这个迭代器就能一条一条取出数据来按 compute chain 来执行一个个transform 操作。iterator 的实现如下：

```
  final def iterator(split: Partition, context: TaskContext): Iterator[T] = {
    if (storageLevel != StorageLevel.NONE) {
      SparkEnv.get.cacheManager.getOrCompute(this, split, context, storageLevel)
    } else {
      computeOrReadCheckpoint(split, context)
    }
  }
```

def 前加了 final 说明该函数是不能被子类重写的，其先判断 RDD 的 storageLevel 是否为 NONE，若不是，则尝试从缓存中读取，读取不到则通过计算来获取该 Partition 对应的数据的迭代器；若是，尝试从 checkpoint 中获取 Partition 对应数据的迭代器，若 checkpoint 不存在则通过计算来获取。

刚刚介绍了如果从 cache 或者 checkpoint 无法获得 Partition 对应的数据的迭代器，则需要通过计算来获取，这将会调用到 ```def compute(split: Partition, context: TaskContext): Iterator[T]``` 方法，各个 RDD 最大的不同也体现在该方法中。后文会详细介绍该方法

##(二): partitioner
partitioner 即分区器，说白了就是决定 RDD 的每一条消息应该分到哪个分区。但只有 k, v 类型的 RDD 才能有 partitioner（当然，非 key， value 类型的 RDD 的 partitioner 为 None。

partitioner 为 None 的 RDD 的 partition 的数据要么对应数据源的某一段数据，要么来自对父 RDDs 的 partitions 的处理结果。

我们先来看看 Partitioner 的定义及注释说明：

```
abstract class Partitioner extends Serializable {
  //< 返回 partition 数量
  def numPartitions: Int
  //< 返回 key 应该属于哪个 partition
  def getPartition(key: Any): Int
}
```

Partitioner 共有两种实现，分别是 HashPartitioner 和 RangePartitioner

###HashPartitioner
先来看 HashPartitioner 的实现（省去部分代码）：

```
class HashPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
  }

  ...
}

// x 对 mod 求于，若结果为正，则返回该结果；若结果为负，返回结果加上 mod
def nonNegativeMod(x: Int, mod: Int): Int = {
  val rawMod = x % mod
  rawMod + (if (rawMod < 0) mod else 0)
}
```

```numPartitions``` 直接返回主构造函数中传入的 partitions 参数，之前在有本书里看到说 Partitioner 不仅决定了一条 record 应该属于哪个 partition，还决定了 partition 的数量，其实这句话的后半段的有误的，Partitioner 并不能决定一个 RDD 的 partition 数，Partitioner 方法返回的 partition 数是直接返回外部传入的值。

```getPartition``` 方法也不复杂，主要做了：

1. 为参数 key 计算一个 hash 值
2. 若该哈希值对 partition 个数取余结果为正，则该结果即该 key 归属的 partition index；否则，以该结果加上 partition 个数为 partition index

从上面的分析来看，当 key， value 类型的 RDD 的 key 的 hash 值分布不均匀时，会导致各个 partition 的数据量不均匀，极端情况下一个 partition 会持有整个 RDD 的数据而其他 partition 则不包含任何数据，这显然不是我们希望看到的，这时就需要 RangePartitioner 出马了。

###RangePartitioner
上文也提到了，HashPartitioner 可能会导致各个 partition 数据量相差很大的情况。这时，初衷为使各个 partition 数据分布尽量均匀的 RangePartitioner 便有了用武之地。

RangePartitioner 将一个范围内的数据映射到 partition，这样两个 partition 之间要么是一个 partition 的数据都比另外一个大，或者小。RangePartitioner采用水塘抽样算法，比 HashPartitioner 耗时，具体可见：[Spark分区器HashPartitioner和RangePartitioner代码详解](http://www.iteblog.com/archives/1522)

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
