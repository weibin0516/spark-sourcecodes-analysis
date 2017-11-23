> Spark 2.0 中已经移除 Hash Based Shuffle，但作为曾经的默认 Shuffle 机制，还是值得进行分析

Spark 最开始只有 Hash Based Shuffle，因为在很多场景中并不需要排序，在这些场景中多余的排序反而会损耗性能。



## Hash Based Shuffle Write
该过程实现的核心是在 ```HashShuffleWriter#write(records: Iterator[Product2[K, V]]): Unit``` 其主要流程如下：


![](http://upload-images.jianshu.io/upload_images/204749-c434b3b8b6fcecec.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


该函数的输入是一个 Shuffle Map Task 计算得到的结果（对应的迭代器），若在宽依赖中定义了 map 端的聚合则会先进行聚合，随后对于迭代器（若要聚合则为聚合后的迭代器）的每一项先通过计算 key 的 hash 值来确定要写到哪个文件，然后将 key、value 写入文件。

写入的文件名的格式是：```shuffle_$shuffleId_$mapId_$reduceId```。写入时，若文件已存在会删除会创建新文件。

上图描述了如何处理一个 Shuffle Map Task 计算结果，在实际应用中，往往有很多 Shuffle Map Tasks 及下游 tasks，即如下情况（图摘自：[JerryLead/SparkInternals-Shuffle 过程](https://github.com/JerryLead/SparkInternals/blob/master/markdown/4-shuffleDetails.md)）：


![](http://upload-images.jianshu.io/upload_images/204749-453334071616f4a1.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


### 存在的问题
这种简单的实现会有几个问题，为说明方便，这里设 ```M = Shuffle Map Task 数量```，```R = 下游 tasks 数量```：

* 产生过多文件：由于每个 Shuffle Map Task 需要为每个下游的 Task 创建一个单独的文件，因此文件的数量就是 ```M * R```。如果 Shuffle Map Tasks 数量是 1000，下游的 tasks 数是 800，那么理论上会产生 80w 个文件（对于 size 为 0的文件会特殊处理）
* 打开多个文件对于系统来说意味着随机写，尤其是每个文件较小且文件特别多的情况。机械硬盘在随机读写方面的性能很差，如果是固态硬盘，会改善很多
* 缓冲区占用内存空间大：每个 Shuffle Map Task 需要开 R 个 bucket（为减少写文件次数的缓冲区），N 个 Shuffle Map Task 就会产生 ```N * R``` 个 bucket。虽然一个 Shuffle Map Task，对应的 buckets 会被回收，但一个节点上的 bucket 个数最多可以达到 ```cores * R``` 个，每个 bucket 默认为 32KB。对于 24 核 1000 个 reducer 来说，占用内存就是 750MB

## 改进：Shuffle Consolidate Writer
在上面提到的几个问题，Spark 提供了 Shuffle Consolidate Files 机制进行优化。该机制的手段是减少 Shuffle 过程产生的文件，若使用这个功能，则需要置 ```spark.shuffle.consolidateFiles``` 为 ```true```，其实现可用下图来表示（图摘自：[JerryLead/SparkInternals-Shuffle 过程](https://github.com/JerryLead/SparkInternals/blob/master/markdown/4-shuffleDetails.md)）


![](http://upload-images.jianshu.io/upload_images/204749-3a09e95b720e2a31.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


即：对于运行在同一个 core 的 Shuffle Map Tasks，对于将要被同一个 reducer read 的数据，第一个 Shuffle Map Task 会创建一个文件，之后的就会将数据追加到这个文件而不是新建一个文件（相当于同一个 core 上的 Shuffle Map Task 写了文件不同的部分）。因此文件数就从原来的 ```M * R``` 个变成了 ```cores * R``` 个。当 ```M / cores``` 的值越大，减少文件数的效果越显著。需要注意的是，该机制虽然在很多时候能缓解上述的几个问题，但是并不能彻底解决。

## 参考
* 《Spark 技术内幕》
* [JerryLead/SparkInternals - Shuffle 过程](https://github.com/JerryLead/SparkInternals/blob/master/markdown/4-shuffleDetails.md)

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
