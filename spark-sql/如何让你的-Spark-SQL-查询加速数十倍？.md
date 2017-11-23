先来回答标题所提的问题，这里的答案是列存储，下面对列存储及在列存储加速 Spark SQL 查询速度进行介绍

## 列存储
### 什么是列存储
传统的数据库通常以行单位做数据存储，而列式存储（后文均以列存储简称）以列为单位做数据存储，如下：


![](http://upload-images.jianshu.io/upload_images/204749-eeaefd09414fbe0e.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


![](http://upload-images.jianshu.io/upload_images/204749-b552a887b57fef00.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

### 优势
列存储相比于行存储主要有以下几个优势：

* 数据即索引，查询是可以跳过不符合条件的数据，只读取需要的数据，降低 IO 数据量（行存储没有索引查询时造成大量 IO，建立索引和物化视图代价较大）
* 只读取需要的列，进一步降低 IO 数据量，加速扫描性能（行存储会扫描所有列）
* 由于同一列的数据类型是一样的，可以使用高效的压缩编码来节约存储空间

当然列存储并不是在所有场景都强于行存储，当查询要读取多个列时，行存储一次就能读取多列，而列存储需要读取多次。Spark 原始支持 parquet 和 orc 两个列存储，下文的实践使用 parquet

## 使用 Parquet 加速 Spark SQL 查询
在我的实践中，使用的 Spark 版本是 2.0.0，测试数据集包含1.18亿条数据，44G，每条数据共有17个字段，假设字段名是 f1,f2...f17。

使用 Parquet 格式的列存储主要带来三个好处

#### 大大节省存储空间
使用行存储占用 44G，将行存储转成 parquet 后仅占用 5.6G，节省了 87.2% 空间，使用 Spark 将数据转成列存储耗时4分钟左右（该值与使用资源相关）

### 只读取指定行
Sql: ```select count(distinct f1) from tbInRow/tbInParquet```

行存储耗时: 119.7s
列存储耗时: 3.4s
加速 35 倍

### 跳过不符合条件数据
Sql: ```select count(f1) from tbInRow/tbInParquet where f1 > 10000```

行存储耗时: 102.8s
列存储耗时: 1.3s
加速 78 倍

**当然，上文也提到了，列存储在查询需要读取多列时并不占优势：**
Sql: ```select f1, f2, f3...f17 from tbInRow/tbInParquet limit 1```

行存储耗时: 1.7s
列存储耗时: 1.9s

列存储带来的加速会因为不同的数据，不同的查询，不同的资源情况而不同，也许在你的实践中加速效果可能不如或比我这里例子的更好，这需要我们根据列存储的特性来善用之

## 参考
* http://www.infoq.com/cn/articles/in-depth-analysis-of-parquet-column-storage-format
* http://chattool.sinaapp.com/?p=1234

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
