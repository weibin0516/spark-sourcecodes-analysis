例子如下:

```
scala> val textFileRDD = sc.textFile("/Users/zhuweibin/Downloads/hive_04053f79f32b414a9cf5ab0d4a3c9daf.txt")
15/08/03 07:00:08 INFO MemoryStore: ensureFreeSpace(57160) called with curMem=0, maxMem=278019440
15/08/03 07:00:08 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 55.8 KB, free 265.1 MB)
15/08/03 07:00:08 INFO MemoryStore: ensureFreeSpace(17237) called with curMem=57160, maxMem=278019440
15/08/03 07:00:08 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 16.8 KB, free 265.1 MB)
15/08/03 07:00:08 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on localhost:51675 (size: 16.8 KB, free: 265.1 MB)
15/08/03 07:00:08 INFO SparkContext: Created broadcast 0 from textFile at <console>:21
textFileRDD: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[1] at textFile at <console>:21

scala>     println( textFileRDD.partitions.size )
15/08/03 07:00:09 INFO FileInputFormat: Total input paths to process : 1
2

scala>     textFileRDD.partitions.foreach { partition =>
     |       println("index:" + partition.index + "  hasCode:" + partition.hashCode())
     |     }
index:0  hasCode:1681
index:1  hasCode:1682

scala>     println("dependency size:" + textFileRDD.dependencies)
dependency size:List(org.apache.spark.OneToOneDependency@543669de)

scala>     println( textFileRDD )
MapPartitionsRDD[1] at textFile at <console>:21

scala>     textFileRDD.dependencies.foreach { dep =>
     |       println("dependency type:" + dep.getClass)
     |       println("dependency RDD:" + dep.rdd)
     |       println("dependency partitions:" + dep.rdd.partitions)
     |       println("dependency partitions size:" + dep.rdd.partitions.length)
     |     }
dependency type:class org.apache.spark.OneToOneDependency
dependency RDD:/Users/zhuweibin/Downloads/hive_04053f79f32b414a9cf5ab0d4a3c9daf.txt HadoopRDD[0] at textFile at <console>:21
dependency partitions:[Lorg.apache.spark.Partition;@c197f46
dependency partitions size:2

scala> 

scala>     val flatMapRDD = textFileRDD.flatMap(_.split(" "))
flatMapRDD: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[2] at flatMap at <console>:23

scala>     println( flatMapRDD )
MapPartitionsRDD[2] at flatMap at <console>:23

scala>     flatMapRDD.dependencies.foreach { dep =>
     |       println("dependency type:" + dep.getClass)
     |       println("dependency RDD:" + dep.rdd)
     |       println("dependency partitions:" + dep.rdd.partitions)
     |       println("dependency partitions size:" + dep.rdd.partitions.length)
     |     }
dependency type:class org.apache.spark.OneToOneDependency
dependency RDD:MapPartitionsRDD[1] at textFile at <console>:21
dependency partitions:[Lorg.apache.spark.Partition;@c197f46
dependency partitions size:2

scala> 

scala>     val mapRDD = flatMapRDD.map(word => (word, 1))
mapRDD: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[3] at map at <console>:25

scala>     println( mapRDD )
MapPartitionsRDD[3] at map at <console>:25

scala>     mapRDD.dependencies.foreach { dep =>
     |       println("dependency type:" + dep.getClass)
     |       println("dependency RDD:" + dep.rdd)
     |       println("dependency partitions:" + dep.rdd.partitions)
     |       println("dependency partitions size:" + dep.rdd.partitions.length)
     |     }
dependency type:class org.apache.spark.OneToOneDependency
dependency RDD:MapPartitionsRDD[2] at flatMap at <console>:23
dependency partitions:[Lorg.apache.spark.Partition;@c197f46
dependency partitions size:2

scala> 

scala> 

scala>     val counts = mapRDD.reduceByKey(_ + _)
counts: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[4] at reduceByKey at <console>:27

scala>     println( counts )
ShuffledRDD[4] at reduceByKey at <console>:27

scala>     counts.dependencies.foreach { dep =>
     |       println("dependency type:" + dep.getClass)
     |       println("dependency RDD:" + dep.rdd)
     |       println("dependency partitions:" + dep.rdd.partitions)
     |       println("dependency partitions size:" + dep.rdd.partitions.length)
     |     }
dependency type:class org.apache.spark.ShuffleDependency
dependency RDD:MapPartitionsRDD[3] at map at <console>:25
dependency partitions:[Lorg.apache.spark.Partition;@c197f46
dependency partitions size:2

scala>
```

从输出我们可以看出，对于任意一个RDD x来说，其dependencies代表了其直接依赖的RDDs（一个或多个）。那dependencies又是怎么能够表明RDD之间的依赖关系呢？假设dependency为dependencies成员

* dependency的类型（NarrowDependency或ShuffleDependency）说明了该依赖是窄依赖还是宽依赖
* 通过dependency的```def getParents(partitionId: Int): Seq[Int]```方法，可以得到子RDD的每个分区依赖父RDD的哪些分区
* dependency包含RDD成员，即子RDD依赖的父RDD，该RDD的compute函数说明了对该父RDD的分区进行怎么样的计算能得到子RDD的分区
* 该父RDD中同样包含dependency成员，该dependency同样包含上述特点，同样可以通过该父RDD的dependency成员来确定该父RDD依赖的爷爷RDD。同样可以通过```dependency.getParents```方法和爷爷RDD.compute来得出如何从父RDD回朔到爷爷RDD，依次类推，可以回朔到第一个RDD

那么，如果某个RDD的partition计算失败，要回朔到哪个RDD为止呢？上例中打印出的dependency.RDD如下：

```
MapPartitionsRDD[1] at textFile at <console>:21
MapPartitionsRDD[2] at flatMap at <console>:23
MapPartitionsRDD[3] at map at <console>:25
ShuffledRDD[4] at reduceByKey at <console>:27
```

可以看出每个RDD都有一个编号，在回朔的过程中，每向上回朔一次变回得到一个或多个相对父RDD，这时系统会判断该RDD是否存在（即被缓存），如果存在则停止回朔，如果不存在则一直向上回朔到某个RDD存在或到最初RDD的数据源为止。

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
