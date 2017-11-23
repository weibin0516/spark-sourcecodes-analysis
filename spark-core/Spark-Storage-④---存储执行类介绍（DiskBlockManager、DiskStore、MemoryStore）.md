> 本文为 Spark 2.0 源码分析笔记，某些实现可能与其他版本有所出入

这篇文章前半部分我们对直接在 Block 存取发挥重要作用的类进行介绍，主要是 DiskBlockManager、MemoryStore、DiskStore。后半部分以存取 Broadcast 来进一步加深对 Block 存取的理解。

## DiskBlockManager
DiskBlockManager 主要用来创建并持有逻辑 blocks 与磁盘上的 blocks之间的映射，一个逻辑 block 通过 BlockId 映射到一个磁盘上的文件。

### 主要成员
* ```localDirs: Array[File]```：创建根据 ```spark.local.dir``` （备注①）指定的目录列表，这些目录下会创建子目录，这些子目录用来存放 Application 运行过程中产生的存放在磁盘上的中间数据，比如 cached RDD partition 对应的 block、Shuffle Write 产生的数据等，会根据文件名将 block 文件 hash 到不同的目录下
* ```subDirs: Array.fill(localDirs.length)(new Array[File](subDirsPerLocalDir))```：localDirs 代表的各个目录下的子目录，子目录个数由 ```spark.diskStore.subDirectories``` 指定，子目录用来存储具体的 block 对应的文件，会根据 block file 文件名先 hash 确定放在哪个 localDir，在 hash 决定放在该 localDir 的哪个子目录下（寻找该 block 文件也是通过这种方式）
* ```shutdownHook = addShutdownHook()```：即关闭钩子，在进程结束时会递归删除 localDirs 下所有属于该 Application 的文件

### 主要方法
看了上面几个主要成员的介绍相信已经对逻辑 block 如何与磁盘文件映射已经有了大致了解。接下来看看几个主要的方法：

* ```getFile(filename: String): File```：通过文件名来查找 block 文件并获取文件句柄，先通过文件名 hash 到指定目录再查找
* ```getFile(blockId: BlockId): File```：通过 blockId 来查找 block 文件并获取文件句柄，事实上是通过调用 ```getFile(filename: String): File``` 来查找的
* ```containsBlock(blockId: BlockId): Boolean```：是否包含某个 blockId 对应的文件
* ```getAllFiles(): Seq[File]```：获取存储在磁盘上所有 block 文件的句柄，以列表的形式返回
* ```getAllBlocks(): Seq[BlockId]```：获取存储在磁盘上的所有 blockId
* ```stop(): Unit```：清理存储在磁盘上所有的 block 文件
* ```createTempLocalBlock(): (TempLocalBlockId, File)```：产生一个唯一的 Block Id 和文件句柄用于存储本地中间结果
* ```createTempShuffleBlock(): (TempShuffleBlockId, File)```：产生一个唯一的 Block Id 和文件句柄用于存储 shuffle 中间结果

如上述，DiskBlockManager 提供的方法主要是为了提供映射的方法，而并不会将现成的映射关系保存在某个成员中，这是需要明了的一点。DiskBlockManager 方法主要在需要创建或获取某个 block 对应的磁盘文件以及在 BlockManager 退出时要清理磁盘文件时被调用。

---

## DiskStore
DiskStore 用来将 block 数据存储至磁盘，是直接的磁盘文件操作者。其封装了：

### 两个写方法
* ```put(blockId: BlockId)(writeFunc: FileOutputStream => Unit): Unit```：用文件输出流的方式写 block 数据至磁盘
* ```putBytes(blockId: BlockId, bytes: ChunkedByteBuffer): Unit```：以字节 buffer 的方式写 block 数据至磁盘

### 一个读方法
* ```getBytes(blockId: BlockId): ChunkedByteBuffer```：通过 block id 读取存储在磁盘上的 block 数据，以字节 buffer 的形式返回

### 两个查方法
* ```getSize(blockId: BlockId): Long```：通过 block id 获取存储在磁盘上的 block 数据的大小
* ```contains(blockId: BlockId): Boolean```：查询磁盘上是否包含某个 block id 的数据

### 一个删方法
* ```remove(blockId: BlockId): Boolean```：删除磁盘上某个 block id 的数据

需要说明的是，DiskStore 的各个方法中，通过 block id 或文件名来找到对应的 block 文件句柄是通过调用 DiskBlockManager 的方法来达成的

---

## MemoryStore
MemoryStore 用来将没有序列化的 Java 对象数组和序列化的字节 buffer 存储至内存中。它的实现比 DiskStore 稍复杂，我们先来看看主要成员

先说明 ```MemoryEntry```：

```
private sealed trait MemoryEntry[T] {
  def size: Long
  def memoryMode: MemoryMode
  def classTag: ClassTag[T]
}

public enum MemoryMode {
  ON_HEAP,
  OFF_HEAP
}
```

代表 JVM 或对外内存的内存大小

### 主要成员
* ```entries: LinkedHashMap[BlockId, MemoryEntry[_]]```：保存每个 block id 及其存储在内存中的数据的大小及是保存在 JVM 内存中还是堆外内存中
* ```unrollMemoryMap: mutable.HashMap[Long, Long]```：保存每个 task 占用的用来存储 block 而占用的 JVM 内存
* ```offHeapUnrollMemoryMap: mutable.HashMap[Long, Long]```：保存每个 task 占用的用来存储 block 而占用的对外内存

以上几个成员主要描述了每个 block 占用了多少内存空间，每个 task 占用了多少内存空间以及它们占用的是 JVM 内存还是堆外内存。接下来看看几个重要的方法：

### 三个写方法
* ```putBytes[T: ClassTag](blockId: BlockId, size: Long, memoryMode: MemoryMode, _bytes: () => ChunkedByteBuffer): Boolean```：先检查是否还有空余内存来存储参数 size 这么大的 block，若有则将 block 以字节 buffer 形式存入；否则不存入，返回失败
* ```putIteratorAsValues[T](blockId: BlockId, values: Iterator[T], classTag: ClassTag[T]): Either[PartiallyUnrolledIterator[T], Long]```：尝试将参数 blockId 对应的数据通过迭代器的方式写入内存。为避免由于空余内存不足以存放 block 数据而导致的 OOM。该方法会逐步展开迭代器来检查是否还有空余内存。如果迭代器顺利展开了，那么用来展开迭代器的内存直接转换为存储内存，而不用再去分配内存来存储该 block 数据。如果未能完全开展迭代器，则返回一个包含 block 数据的迭代器，其对应的数据是由多个局部块组合而成的 block 数据
* ```putIteratorAsBytes[T](blockId: BlockId, values: Iterator[T], classTag: ClassTag[T], memoryMode: MemoryMode): Either[PartiallySerializedBlock[T], Long]```：尝试将参数 blockId 对应的数据通过字节 buffer 的方式写入内存。为避免由于空余内存不足以存放 block 数据而导致的 OOM。该方法会逐步展开迭代器来检查是否还有空余内存。如果迭代器顺利展开了，那么用来展开迭代器的内存直接转换为存储内存，而不用再去分配内存来存储该 block 数据。如果未能完全开展迭代器，则返回一个包含 block 数据的迭代器，其对应的数据是由多个局部块组合而成的 block 数据

### 两个读方法
* ```getBytes(blockId: BlockId): Option[ChunkedByteBuffer]```：以字节 buffer 的形式获取参数 blockId 指定的 block 数据
* ```getValues(blockId: BlockId): Option[Iterator[_]]```：以迭代器的形式获取参数 blockId 指定的 block 数据

### 若干个查方法
* ```getSize(blockId: BlockId): Long```：获取 blockId 对应 block 占用的内存大小
* ```contains(blockId: BlockId): Boolean```：内存中是否包含某个 blockId 对应的 block 数据
* ```currentUnrollMemory(): Long```：当前所有 tasks 用于存储 blocks 占用的总内存
* ...

### 两个删方法
* ```remove(blockId: BlockId): Boolean```：删除内存中 blockId 指定的 block 数据
* ```clear(): Unit```：清除 MemoryStore 中存储的所有 blocks 数据

从上面描述的 MemoryStore 的主要方法来看，其功能和 DiskStore 类似，但由于要考虑到 JVM 内存和堆外内存以及有可能内存不足以存储 block 数据等问题会变得更加复杂

---

## 备注说明
* 备注①：设置 ```spark.local.dir``` 时可以设置多个目录，目录分别在不同磁盘上，可以增加整体 IO 带宽；也尽量让目录位于更快的磁盘上以获得更快的 IO 速度

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
