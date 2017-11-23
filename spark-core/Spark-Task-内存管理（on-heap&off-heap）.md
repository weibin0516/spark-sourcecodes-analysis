> 本文为 Spark 2.0 源码分析，其他版本可能会有所不同

在之前的文章中（[Spark 新旧内存管理方案（上）](http://www.jianshu.com/p/2e9eda28e86c)及[Spark 新旧内存管理方案（下）](http://www.jianshu.com/p/bb0bdcb26ccc)），我从粗粒度上对 Spark 内存管理进行了剖析，但我们依然会有类似这样的疑问，在 task 中，shuffle 时使用的内存具体是怎么分配的？是在堆上分配的还是堆外分配的？堆上如何分配、堆外又如何分配？

这些问题可以通过剖析 TaskMemoryManager 来解决。TaskMemoryManager 用来管理一个 task 的内存，主要涉及申请内存、释放内存及如何表示统一的表示从堆或堆外申请的一块固定大小的连续的内存

### 统一的内存块表示 - MemoryBlock

对于堆内存，分配、释放及对象引用关系都由 JVM 进行管理。new 只是返回一个对象引用，而不是该对象在进程地址空间的地址。堆内存的使用严重依赖 JVM 的 GC 器，对于大内存的使用频繁的 GC 经常会对性能造成很大影响。

Java 提供的 ByteBuffer.allocateDirect 方法可以分配堆外内存，其分配大小受 ```MaxDirectMemorySize``` 配置限制。另一种分配堆外内存的方法就是 Unsafe 的 ```allocateMemory``` 方法，相比前者，它完全脱离了 JVM 限制，与 C 中的 malloc 功能一致。这两个方法还有另一个区别：后者返回的是进程空间的实际内存地址，而前者被 ByteBuffer 进行包装。

堆内内存使用简单，但在使用大内存时其 GC 机制容易影响性能；堆外内存相交于堆内存使用复杂，但精确的内存控制使其更高效。在 Spark 中，很多地方会有大数组大内存的需求，高效的内存使用时必须的，因此 Spark 也提供了堆外内存的支持，以优化 Application 运行性能。

Spark 封装了 ```MemoryLocation``` 来表示一个逻辑内存地址，其定义如下：

```
public class MemoryLocation {
  Object obj;
  long offset;

  //< 适用于堆内内存
  public MemoryLocation(@Nullable Object obj, long offset) {
    this.obj = obj;
    this.offset = offset;
  }

  //< 适用于堆外内存
  public MemoryLocation() {
    this(null, 0);
  }

  ...
}

```

以及 MemoryBlock 来表示一块连续的内存，这块内存可以从堆或对外分配，包含以下成员：

* length：内存块大小
* pageNumber：page id（这块内存又被叫做 page）
* obj：见下文分析
* offset：见下文分析

```
public class MemoryBlock extends MemoryLocation {

  private final long length;
  public int pageNumber = -1;

  public MemoryBlock(@Nullable Object obj, long offset, long length) {
    super(obj, offset);
    this.length = length;
  }

  ...
}
```

接下来我们来看看如何从堆内和堆外申请内存并生成对应的 MemoryBlock 对象。

#### 申请堆外内存
Spark 封装了 ```UnsafeMemoryAllocator``` 类来分配和释放堆外内存，分配的方法如下：

```
  public MemoryBlock allocate(long size) throws OutOfMemoryError {
    long address = Platform.allocateMemory(size);
    return new MemoryBlock(null, address, size);
  }
```

其中 ```Platform.allocateMemory(size)``` 会调用 ```Unsafe.allocateMemory``` 来从堆外分配一块 size 大小的内存并返回其绝对地址。随后，构造并返回 MemoryBlock 对象，需要注意的是，**该对象的 obj 成员为 ```null```，offset 成员为该绝对地址**

#### 申请堆内存
Spark 封装了 ```HeapMemoryAllocator``` 类分配和释放堆内存，分配的方法如下：

```
  public MemoryBlock allocate(long size) throws OutOfMemoryError {
    ...
    long[] array = new long[(int) ((size + 7) / 8)];
    return new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, size);
  }
```

总共分为两步：

1. 以8字节对齐的方式申请长度为 ```((size + 7) / 8)``` 的 long 数组，得到 array
2. 构造 MemoryBlock 对象，**其 obj 成员为 array，offset 成员为 ```Platform.LONG_ARRAY_OFFSET```**

## Page table
在 TaskMemoryManager 有一个如下成员：

```
private final MemoryBlock[] pageTable = new MemoryBlock[8192];
```

该成员保存着一个 task 所申请的所有 pages（page 即 MemoryBlock），最多可以有8192个。在 ```TaskMemoryManager#allocatePage(...)``` 中从堆或堆外分配的 page 会被添加到该 pageTable 中。

## 对 page 地址进行统一编码
通过上面的分析我们知道，page 对应的内存可能来自堆或堆外。但这显然不应该由上层操作者来操心，所以 ```TaskMemoryManager``` 提供了只需传入 page 及要访问该 page 上的 offset 就能获得一个 long 型的地址。这样应用者只需操作自该地址起的某一段内存即可，而不用关心这块内存是来自哪。这即是 ```TaskMemoryManager``` 提供的 page 地址统一编码，由 ```TaskMemoryManager#encodePageNumberAndOffset(MemoryBlock page, long offsetInPage): long``` 实现

## 参考

* http://blog.csdn.net/lipeng_bigdata/article/details/50752297
* http://www.jianshu.com/p/34729f9f833c
* https://github.com/ColZer/DigAndBuried/blob/master/spark/spark-memory-manager.md

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
