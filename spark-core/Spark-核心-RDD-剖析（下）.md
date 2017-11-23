上文[Spark 核心 RDD 剖析（上）](http://www.jianshu.com/p/207607888767)介绍了 RDD 两个重要要素：partition 和 partitioner。这篇文章将介绍剩余的部分，即 compute func、dependency、preferedLocation

##compute func
在前一篇文章中提到，当调用 ```RDD#iterator``` 方法无法从缓存或 checkpoint 中获取指定 partition 的迭代器时，就需要调用 ```compute``` 方法来获取，该方法声明如下：

```
def compute(split: Partition, context: TaskContext): Iterator[T]
```

每个具体的 RDD 都必须实现自己的 compute 函数。从上面的分析我们可以联想到，任何一个 RDD 的任意一个 partition 都首先是通过 compute 函数计算出的，之后才能进行 cache 或 checkpoint。接下来我们来对几个常用 transformation 操作对应的 RDD 的 compute 进行分析

###map
首先来看下 map 的实现：

```
  def map[U: ClassTag](f: T => U): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.map(cleanF))
  }
```

我们调用 map 时，会传入匿名函数 ```f: T => U```，该函数将一个类型 T 实例转换成一个类型 U 的实例。在 map 函数中，将该函数进一步封装成 ```(context, pid, iter) => iter.map(cleanF)``` 的函数，该函数以迭代器作为参数，对迭代出的每一个元素执行 f 函数，然后以该封装后的函数作为参数来构造 MapPartitionsRDD，接下来看看 ```MapPartitionsRDD#compute``` 是怎么实现的：

```
  override def compute(split: Partition, context: TaskContext): Iterator[U] =
    f(context, split.index, firstParent[T].iterator(split, context))
```

上面代码中的 firstParent 是指本 RDD 的依赖 ```dependencies: Seq[Dependency[_]]``` 中的第一个，MapPartitionsRDD 的依赖中只有一个父 RDD。而 MapPartitionsRDD 的 partition 与其唯一的父 RDD partition 是一一对应的，所以其 compute 方法可以描述为：对父 RDD partition 中的每一个元素执行传入 map 的方法得到自身的 partition 及迭代器

###groupByKey
与 map、union 不同，groupByKey 是一个会产生宽依赖的 transform，其最终生成的 RDD 是 ShuffledRDD，来看看其 compute 实现：

```
  override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
      .read()
      .asInstanceOf[Iterator[(K, C)]]
  }
```

可以看到，ShuffledRDD 的 compute 使用 ShuffleManager 来获取一个 reader，该 reader 将从本地或远程 BlockManager 拉取 map output 的 file 数据，每个 reduce task 拉取一个 partition 数据。

对于其他生成 ShuffledRDD 的 transform 的 compute 操作也是如此，比如 reduceByKey，join 等

##dependency
RDD 依赖是一个 Seq 类型：```dependencies_ : Seq[Dependency[_]]```，因为一个 RDD 可以有多个父 RDD。共有两种依赖：

* 窄依赖：父 RDD 的 partition 至多被一个子 RDD partition 依赖
* 宽依赖：父 RDD 的 partition 被多个子 RDD partitions 依赖

窄依赖共有两种实现，一种是一对一的依赖，即 OneToOneDependency：

```
@DeveloperApi
class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  override def getParents(partitionId: Int): List[Int] = List(partitionId)
}
```

从其 getParents 方法可以看出 OneToOneDependency 的依赖关系下，子 RDD 的 partition 仅依赖于唯一 parent RDD 的相同 index 的 partition。另一种窄依赖的实现是 RangeDependency，它仅仅被 UnionRDD 使用，UnionRDD 把多个 RDD 合成一个 RDD，这些 RDD 是被拼接而成，其 getParents 实现如下：

```
  override def getParents(partitionId: Int): List[Int] = {
    if (partitionId >= outStart && partitionId < outStart + length) {
      List(partitionId - outStart + inStart)
    } else {
      Nil
    }
  }
```

宽依赖只有一种实现，即 ShuffleDependency，宽依赖支持两种 Shuffle Manager，即 ```HashShuffleManager``` 和 ```SortShuffleManager```，Shuffle 相关内容以后会专门写文章介绍

##preferedLocation
preferedLocation 即 RDD 每个 partition 对应的优先位置，每个 partition 对应一个```Seq[String]```，表示一组优先节点的 host。

要注意的是，并不是每个 RDD 都有 preferedLocation，比如从 Scala 集合中创建的 RDD 就没有，而从 HDFS 读取的 RDD 就有，其 partition 对应的优先位置及对应的 block 所在的各个节点。

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
