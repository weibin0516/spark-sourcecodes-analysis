> 撰写本文时 Spark 的最新版本为 2.0.0

## 概述
Spark SQL 是 Spark 用来处理结构化数据的一个模块。与基础的 Spark RDD API 不同，Spark SQL 提供了更多数据与要执行的计算的信息。在其实现中，会使用这些额外信息进行优化。可以使用 SQL 语句和 Dataset API 来与 Spark SQL 模块交互。无论你使用哪种语言或 API 来执行计算，都会使用相同的引擎。这让你可以选择你熟悉的语言（现支持 Scala、Java、R、Python）以及在不同场景下选择不同的方式来进行计算。

### SQL
一种使用 Spark SQL 的方式是使用 SQL。Spark SQL 也支持从 Hive 中读取数据，如何配置将会在下文中介绍。使用编码方式来执行 SQL 将会返回一个 Dataset/DataFrame。你也可以使用命令行，JDBC/ODBC 与 Spark SQL 进行交互。

### Datasets 和 DataFrames
Dataset 是一个分布式数据集合。Dataset 是自 Spark 1.6开始提供的新接口，能同时享受到 RDDs 的优势（强类型，能使用强大的 lambda 函数）以及 Spark SQL 优化过的执行引擎。Dataset 可以从 JVM 对象(s)创建而来并且可以使用各种 transform 操作（比如 map，flatMap，filter 等）。目前 Dataset API 支持 Scala 和 Java。Python 暂不支持 Dataset API。不过得益于 Python 的动态属性，可以享受到许多 DataSet API 的益处。R 也是类似情况。

DataFrame 是具有名字的列。概念上相当于关系数据库中的表或 R/Python 下的 ```data frame```，但有更多的优化。DataFrames（Dataset 亦是如此） 可以从很多数据中构造，比如：结构化文件、Hive 中的表，数据库，已存在的 RDDs。DataFrame API 可在 Scala、Java、Python 和 R 中使用。在 Scala 和 Java 中，DataFrame 由一个元素为 Row 的 Dataset 表示。在 Scala API 中，DataFrame 只是 ```Dataset[Row]``` 的别名。在 Java API 中，类型为 ```Dataset<Row>```。

在本文剩余篇幅中，会经常使用 DataFrame 来代指 Scala/Java 元素为 Row 的 Dataset。

## 开始
###起始点：SparkSession
SparkSession 类是到 Spark SQL 所有功能的入口点，只需调用 ```SparkSession.builder()``` 即可创建：

```
import org.apache.spark.sql.SparkSession

val spark = SparkSession
    .builder()
    .appName("Spark SQL Example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
    
// 包含隐式转换（比如讲 RDDs 转成 DataFrames）API
import spark.implicits._
```

Spark 2.0中的 SparkSession对于 Hive 的各个特性提供了内置支持，包括使用 HiveQL 编写查询语句，使用 Hive UDFs 以及从 Hive 表中读取数据。

### 创建 DataFrames
使用 SparkSession，可以从已经在的 RDD、Hive 表以及 Spark 支持的数据格式创建。下面这个例子就是读取一个 Json 文件来创建一个 DataFrames：

```
val df = spark.read.json("examples/src/main/resources/people.json")

// Displays the content of the DataFrame to stdout
df.show()
// +----+-------+
// | age|   name|
// +----+-------+
// |null|Michael|
// |  30|   Andy|
// |  19| Justin|
// +----+-------+
```

### 无类型 Dataset 操作（又名 DataFrame 操作）
DataFrames提供特定于域的语言结构化数据操作。如上所述，在 Spark 2.0 中，DataFrames 是元素为 Row 的 Dataset 在 Scala 和 Java API 中。相较于强类型的 Scala/Java Dataset 的“有类型操作”，DataFrame 上的操作又被称为“无类型操作”。

下面给出一些使用 Dataset 处理结构化数据的基本例子：

```
// This import is needed to use the $-notation
import spark.implicits._
// Print the schema in a tree format
df.printSchema()
// root
// |-- age: long (nullable = true)
// |-- name: string (nullable = true)

// Select only the "name" column
df.select("name").show()
// +-------+
// |   name|
// +-------+
// |Michael|
// |   Andy|
// | Justin|
// +-------+

// Select everybody, but increment the age by 1
df.select($"name", $"age" + 1).show()
// +-------+---------+
// |   name|(age + 1)|
// +-------+---------+
// |Michael|     null|
// |   Andy|       31|
// | Justin|       20|
// +-------+---------+

// Select people older than 21
df.filter($"age" > 21).show()
// +---+----+
// |age|name|
// +---+----+
// | 30|Andy|
// +---+----+

// Count people by age
df.groupBy("age").count().show()
// +----+-----+
// | age|count|
// +----+-----+
// |  19|    1|
// |null|    1|
// |  30|    1|
// +----+-----+
```

要查看 Dataset 支持的所有操作，请移步 [Dataset API 文档](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset)。

除了简单的列引用和表达式，Datasets 丰富的函数库还提供了包括字符串操作，日期操作，内容匹配操作等函数。完整的列表请移步[DataFrame 函数列表](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$)

### 创建 Datasets
Dataset 与 RDD 类似，但它使用一个指定的编码器进行序列化来代替 Java 自带的序列化方法或 Kryo 序列化。尽管该编码器和标准序列化是负责将对象转换成字节，编码器是动态生成的，并提供一种格式允许 Spark 直接执行许多操作，比如 filter、sort 和 hash 等而不用将字节数据反序列化成对象。

```
// Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
// you can use custom classes that implement the Product interface
case class Person(name: String, age: Long)

// Encoders are created for case classes
val caseClassDS = Seq(Person("Andy", 32)).toDS()
caseClassDS.show()
// +----+---+
// |name|age|
// +----+---+
// |Andy| 32|
// +----+---+

// Encoders for most common types are automatically provided by importing spark.implicits._
val primitiveDS = Seq(1, 2, 3).toDS()
primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)

//  通过提供一个类，类各个成员名与 Row 各个字段名相对应，DataFrames可以转换为val path = "examples/src/main/resources/people.json"
val peopleDS = spark.read.json(path).as[Person]
peopleDS.show()
// +----+-------+
// | age|   name|
// +----+-------+
// |null|Michael|
// |  30|   Andy|
// |  19| Justin|
// +----+-------+
```

## 与 RDDs 互操作
Spark SQL 支持两种不同的方式将 RDDs 转换为 Datasets。第一种方法是使用反射来推断包含指定类对象元素的 RDD 的模式。利用这种方法能让代码更简洁。

创建 Datasets 的第二种方法通过接口构造一个模式来应用于现有的 RDD。虽然这种方法要少复杂一些，但允许在列及其类型直到运行时才知道的情况下构造 Datasets。

### 使用反射来推断模式
Spark SQL 的 Scala 接口支持将元素类型为 case class 的 RDD 自动转为 DataFrame。case class 定义了表的模式。case class 的参数名将变成对应列的列名。case class 可以嵌套，也可以包含复合类型，比如 Seqs 或 Arrays。元素为 case class 的 RDD 可以转换成 DataFrame 并可以注册为表进而执行 sql 语句查询。

```
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder

// For implicit conversions from RDDs to DataFrames
import spark.implicits._

// Create an RDD of Person objects from a text file, convert it to a Dataframe
val peopleDF = spark.sparkContext
  .textFile("examples/src/main/resources/people.txt")
  .map(_.split(","))
  .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
  .toDF()
// Register the DataFrame as a temporary view
peopleDF.createOrReplaceTempView("people")

// SQL statements can be run by using the sql methods provided by Spark
val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

// The columns of a row in the result can be accessed by field index
teenagersDF.map(teenager => "Name: " + teenager(0)).show()
// +------------+
// |       value|
// +------------+
// |Name: Justin|
// +------------+

// or by field name
teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()
// +------------+
// |       value|
// +------------+
// |Name: Justin|
// +------------+

// No pre-defined encoders for Dataset[Map[K,V]], define explicitly
implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
// Primitive types and case classes can be also defined as
implicit val stringIntMapEncoder: Encoder[Map[String, Int]] = ExpressionEncoder()

// row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
// Array(Map("name" -> "Justin", "age" -> 19))
```

### 编码指定模式
如果不能预先定义 case class（比如，每条记录都是字符串，不同的用户会使用不同的字段），那么可以通过以下三步来创建 DataFrame：

1. 将原始 RDD 转换为 Row RDD
2. 根据步骤1中的 Row 的结构创建对应的 StructType 模式
3. 通过 SparkSession 提供的 createDataFrame 来把第2步创建的模式应用到第一步转换得到的 Row RDD

```
import org.apache.spark.sql.types._

// Create an RDD
val peopleRDD = spark.sparkContext.textFile("examples/src/main/resources/people.txt")

// The schema is encoded in a string
val schemaString = "name age"

// Generate the schema based on the string of schema
val fields = schemaString.split(" ")
  .map(fieldName => StructField(fieldName, StringType, nullable = true))
val schema = StructType(fields)

// Convert records of the RDD (people) to Rows
val rowRDD = peopleRDD
  .map(_.split(","))
  .map(attributes => Row(attributes(0), attributes(1).trim))

// Apply the schema to the RDD
val peopleDF = spark.createDataFrame(rowRDD, schema)

// Creates a temporary view using the DataFrame
peopleDF.createOrReplaceTempView("people")

// SQL can be run over a temporary view created using DataFrames
val results = spark.sql("SELECT name FROM people")

// The results of SQL queries are DataFrames and support all the normal RDD operations
// The columns of a row in the result can be accessed by field index or by field name
results.map(attributes => "Name: " + attributes(0)).show()
// +-------------+
// |        value|
// +-------------+
// |Name: Michael|
// |   Name: Andy|
// | Name: Justin|
// +-------------+
```

## 数据源
Spark SQL 通过 DataFrame 可以操作多种类型数据。DataFrame 可以创建临时表，创建了临时表后就可以在上面执行 sql 语句了。本节主要介绍 Spark 数据源的加载与保存以及一些内置的操作。

### 通用的 Load/Sava 函数
最简单的方式是调用 load 方法加载文件，默认的格式为 parquet（可以通过修改 ```spark.sql.sources.default``` 来指定默认格式）

```
val usersDF = spark.read.load("examples/src/main/resources/users.parquet")
usersDF.select("name", "favorite_color").write.save("namesAndFavColors.parquet")
```

### 手动指定格式
也可以手动指定加载数据的格式以及要保存的数据的格式

```
val peopleDF = spark.read.format("json").load("examples/src/main/resources/people.json")
peopleDF.select("name", "age").write.format("parquet").save("namesAndAges.parquet")
```

### 在文件夹上执行 SQL
除了使用 read API，还可以在对文件夹的所有文件执行 SQL 查询

```
val sqlDF = spark.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`")
```

### 保存模式
执行保存操作时可以指定一个 SaveMode，SaveMode 指定了如果指定的数据已存在该如何处理。需要意识到保存操作不使用锁也不是原子操作。另外，如果指定了覆盖模式，会在写入新数据前将老数据删除


| Scala/Java | 其他语言 | 含义 |
| --- | --- | --- |
| SaveMode.ErrorIfExists (default) | "error" (default) | 当保存一个DataFrame 数据至数据源时，如果该位置数据已经存在，则会抛出一个异常 |
| SaveMode.Append | 	"append" | 当保存一个DataFrame 数据至数据源时，如果该位置数据已经存在，则将DataFrame 数据追加到已存在的数据尾部 |
| SaveMode.Overwrite | "overwrite" | 当保存一个DataFrame 数据至数据源时，如果该位置数据已经存在，则覆盖元数据（先删除元数据，再保存 DataFrame 数据） |
| SaveMode.Ignore | "ignore" | 当保存一个DataFrame 数据至数据源时，如果该位置数据已经存在，则不执行任何操作；若不存在，则保存 DataFrame（有点像 CREATE TABLE IF NOT EXISTS） |

### 保存数据到永久表
DataFrame 也可以通过调用 ```saveAsTable``` 方法将数据保存到 Hive 表中。注意，当前已部署的 hive 不会受到影响。Spark 会创建本地的 metastore（使用 Derby）。与 ```createOrReplaceTempView``` 不同，```saveAsTable``` 会持久化数据并指向 Hive metastore。在你重启 Spark Application 后，永久表依旧存在，只要你连接了保存时相同的 metastore 依旧能访问到完整的数据。用来保存数据到永久表的 DataFrame 可以通过调用 SparkSession 的 table 方法来创建。

```saveAsTable``` 默认会创建一个 “受管理表”，意味着数据的位置都是受 metastore 管理的。当 “受管理表” 被删除，其对应的数据也都会被删除。

### Parquet 格式
Parquet 是很多数据处理系统都支持的列存储格式，其相对于行存储具有以下优势：

* 可以跳过不符合条件的数据，只读取需要的数据，降低 IO 数据量
* 压缩编码可以降低磁盘存储空间。由于同一列的数据类型是一样的，可以使用更高效的压缩编码进一步节省存储空间
* 只读取需要的列，支持向量运算，能够获取更好的扫描性能

Spark SQL 支持读写 Parquet 格式数据。当写 Parquet 数据时，为了兼容性，所有的列会自动转为 nullable

#### 编码读写 Parquet 文件
```
// Encoders for most common types are automatically provided by importing spark.implicits._
import spark.implicits._

val peopleDF = spark.read.json("examples/src/main/resources/people.json")

// DataFrames can be saved as Parquet files, maintaining the schema information
peopleDF.write.parquet("people.parquet")

// Read in the parquet file created above
// Parquet files are self-describing so the schema is preserved
// The result of loading a Parquet file is also a DataFrame
val parquetFileDF = spark.read.parquet("people.parquet")

// Parquet files can also be used to create a temporary view and then used in SQL statements
parquetFileDF.createOrReplaceTempView("parquetFile")
val namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
namesDF.map(attributes => "Name: " + attributes(0)).show()
// +------------+
// |       value|
// +------------+
// |Name: Justin|
// +------------+
```

#### 分区发现
表分区是像 Hive 的这种系统常用的优化方法。在一个分区的表中，数据往往存储在不同的目录，分区列被编码存储在各个分区目录。Parquet 数据源当前支持自动发现和推断分区信息。举个例子，我们可以使用下列目录结构存储上文中提到的人口属性数据至一个分区的表，将额外的两个列 gender 和 country 作为分区列：

```
path
└── to
    └── table
        ├── gender=male
        │   ├── ...
        │   │
        │   ├── country=US
        │   │   └── data.parquet
        │   ├── country=CN
        │   │   └── data.parquet
        │   └── ...
        └── gender=female
            ├── ...
            │
            ├── country=US
            │   └── data.parquet
            ├── country=CN
            │   └── data.parquet
            └── ...
```

当将 ```path/to/table``` 传给 ```SparkSession.read.parquet``` 或 ```SparkSession.read.load``` 时，Spark SQL 会自动从路径中提取分区信息，返回的 DataFrame 的模式如下：

```
root
|-- name: string (nullable = true)
|-- age: long (nullable = true)
|-- gender: string (nullable = true)
|-- country: string (nullable = true)
```

注意，用来分区的列的数据类型是自动推断的，当前支持数字类型和 String 类型。如果你不希望自动推断分区列的类型，将 ```spark.sql.sources.partitionColumnTypeInference.enabled``` 设置为 false 即可，该值默认为 true。若设为 false，则会禁用分区列类型推断而直接设置为 String 类型。

自 Spark 1.6.0 起，分区发现只会发现指定路径下的分区。在上面的例子中，如果用户传入路径 ```path/to/table/gender=male```，则 gender 将不会成为一个分区列。如果用户即只想访问 ```path/to/table/gender=male``` 下的数据，又希望 gender 能成为分区列，可以使用 basePath 选项，如将 basePath 设置为 ```path/to/table/```，依旧传 ```path/to/table/gender=male``` 给```spark.sql.sources.partitionColumnTypeInference.enabled```，那么 gender 依旧会成为分区列。

#### 合并模式
与 ProtocolBuffer，Avro 和 Thrift 类似，Parquet 也支持模式演进。用户可以从简单的模式开始，之后根据需要逐步增加列。通过这种方式，最终可能会形成不同但互相兼容的多个 Parquet 文件。Parquet 数据源现在可以自动检测这种情况并合并这些文件。

由于模式合并是消耗比较高的操作，而且在大多数情况下都不是必要的，自 1.5.0 开始默认关闭该功能。你可以通过以下方式启用：

1. 当读取 Parquet 文件时，将 ```mergeSchema``` 选项设置为 true，下面代码中有示例，或
2. 设置 ```spark.sql.parquet.mergeSchema``` 为 true

```
// This is used to implicitly convert an RDD to a DataFrame.
import spark.implicits._

// Create a simple DataFrame, store into a partition directory
val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")
squaresDF.write.parquet("data/test_table/key=1")

// Create another DataFrame in a new partition directory,
// adding a new column and dropping an existing column
val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")
cubesDF.write.parquet("data/test_table/key=2")

// Read the partitioned table
val mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test_table")
mergedDF.printSchema()

// The final schema consists of all 3 columns in the Parquet files together
// with the partitioning column appeared in the partition directory paths
// root
// |-- value: int (nullable = true)
// |-- square: int (nullable = true)
// |-- cube: int (nullable = true)
// |-- key : int (nullable = true)
```

### Hive 表
Spark SQL 也支持从 Hive 中读取数据以及保存数据到 Hive 中。然后，由于 Hive 有大量依赖，默认部署的 Spark 不包含这些依赖。可以将 Hive 的依赖添加到 classpath，Spark 将自动加载这些依赖。注意，这些依赖也必须分发到各个节点，因为需要通过 Hive 序列化和反序列化库来读取 Hive 数据和将数据写入 Hive。

配置上需要做的是将 hive-site.xml, core-site.xml (如果有安全相关配置) 以及 hdfs-site.xml拷贝到 ```$SPARK_HOME/conf``` 目录下。

当和 Hive 协作时，需要实例化一个支持 Hive 的 SparkSession。即使没有现成部署好的 Hive 依旧可以启用 Hive 支持。当没有使用 hive-site.xml 进行配置时，会自动的在当前目录创建 metastore_db 并在 ```spark.sql.warehouse.dir``` 指定的目录创建一个目录，用作 ```spark-warehouse``` 目录。需要注意的是，hive-site.xml 中的 ```hive.metastore.warehouse.dir``` 自 Spark 2.0.0 依赖被启用，取而代之，使用 ```spark.sql.warehouse.dir``` 来指定 warehouse 数据库的默认目录。另外，你需要给启动该 Application 的用户这个目录的写权限。

```
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

case class Record(key: Int, value: String)

// warehouseLocation points to the default location for managed databases and tables
val warehouseLocation = "file:${system:user.dir}/spark-warehouse"

val spark = SparkSession
  .builder()
  .appName("Spark Hive Example")
  .config("spark.sql.warehouse.dir", warehouseLocation)
  .enableHiveSupport()
  .getOrCreate()

import spark.implicits._
import spark.sql

sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

// Queries are expressed in HiveQL
sql("SELECT * FROM src").show()
// +---+-------+
// |key|  value|
// +---+-------+
// |238|val_238|
// | 86| val_86|
// |311|val_311|
// ...

// Aggregation queries are also supported.
sql("SELECT COUNT(*) FROM src").show()
// +--------+
// |count(1)|
// +--------+
// |    500 |
// +--------+

// The results of SQL queries are themselves DataFrames and support all normal functions.
val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

// The items in DaraFrames are of type Row, which allows you to access each column by ordinal.
val stringsDS = sqlDF.map {
  case Row(key: Int, value: String) => s"Key: $key, Value: $value"
}
stringsDS.show()
// +--------------------+
// |               value|
// +--------------------+
// |Key: 0, Value: val_0|
// |Key: 0, Value: val_0|
// |Key: 0, Value: val_0|
// ...

// You can also use DataFrames to create temporary views within a HiveContext.
val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
recordsDF.createOrReplaceTempView("records")

// Queries can then join DataFrame data with data stored in Hive.
sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()
// +---+------+---+------+
// |key| value|key| value|
// +---+------+---+------+
// |  2| val_2|  2| val_2|
// |  2| val_2|  2| val_2|
// |  4| val_4|  4| val_4|
// ...
```

### 通过 JDBC 连接其他数据库
Spark SQL 也支持通过 JDBC 来访问其他数据库的数据。使用这种方式将返回 DataFrame，并且 Spark SQL 可以轻易处理或与其他数据做 join 操作，所以我们应该优先使用这种方式而不是 JdbcRDD。

在使用时，需要将对应数据库的 JDBC driver 包含到 spark classpath 中。比如下面的例子是通过 Spark Shell 链接 postgre 数据库：

```
bin/spark-shell --driver-class-path postgresql-9.4.1207.jar --jars postgresql-9.4.1207.jar
``` 

远程数据库中的数据可以被加载为 DataFrame 或 Spark SQL 临时表，支持以下选项：


| 选项 | 含义 |
| --- | --- |
| url | 要连接的 JDBC url |
| dbtable | 要读取的 JDBC 库和表。任何在 SQL 查询的 FROM 子句中支持的形式都支持，比如，用括号包括的 SQL 子查询 |
| driver | 用来连接 JDBC url 的 JDBC driver 的类名 |
| partitionColumn, lowerBound, upperBound, numPartitions | 只要为这其中的一个选项指定了值就必须为所有选项都指定值。这些选项描述了多个 workers 并行读取数据时如何分区。lowerBound 和 upperBound 用来指定分区边界，而不是用来过滤表中数据的，因为表中的所有数据都会被读取并分区 |
| fetchSize | 定义每次读取多少条数据，这有助于提升读取的性能和稳定性。如果一次读取过多数据，容易因为网络原因导致失败 |

一个简单的示例如下：

```
val jdbcDF = spark.read.format("jdbc").options(
  Map("url" -> "jdbc:postgresql:dbserver",
  "dbtable" -> "schema.tablename")).load()
```

## 性能调优
对于很多 Application，我们可以通过缓存数据至内存或调整一些选项来进行性能调优。

### 缓存数据至内存
Spark SQL 通过调用 ```spark.cacheTable``` 或 ```dataFrame.cache()``` 来将表以列式形式缓存到内存。Spark SQL会只会缓存需要的列并且会进行压缩以减小内存消耗和 GC 压力。可以调用 ```spark.uncacheTable("tableName")``` 将表中内存中移除。

可以调用 SparkSession 的 ```setConf``` 方法来设置内存缓存的参数：


| 选项 | 默认值 | 含义 |
| --- | --- | --- |
| spark.sql.inMemoryColumnarStorage.compressed | true | 若设置为 true，Spark SQL 会根据每列的类型自动为每列选择一个压缩器进行数据压缩  |
| spark.sql.inMemoryColumnarStorage.batchSize | 10000 | 设置一次处理多少 row，更大的值有助于提升内存使用率和压缩率，但要注意避免 OOMs  |

### 其他配置项
调整以下选项也能改善查询性能，由于一些优化可能会在以后的版本中自动化，所以以下选项可能会在以后被弃用


| 选项名 | 默认值 | 含义 |
| --- | --- | --- |
| spark.sql.files.maxPartitionBytes | 134217728 (128 MB) | 一个分区的最大 size，单位为字节 |
| spark.sql.autoBroadcastJoinThreshold | 10485760 (10 MB) | 自动广播的小表的最大 size，以字节为单位。设置为 -1 可禁用广播表。注意，当前只支持执行了 ```ANALYZE TABLE <tableName> COMPUTE STATISTICS noscan``` 的 Hive Metastore 表 |
| spark.sql.shuffle.partitions | 200 | 执行 join 和聚合操作时，shuffle 操作的分区数 |

## 分布式 SQL 引擎
使用 JDBC/ODBC 或命令行接口，Spark SQL 还可以作为一个分布式查询引擎。在该模式下，终端用户或 Application 可以直接执行 SQL 查询，而不用写任何代码。

### JDBC/ODBC thrift 服务
这里的 JDBC/ODBC 服务对应于 Hive 1.2.1 中的 HiveServer2，可以通过 beeline 脚本来测试特服务。首先执行下面的命令启动 JDBC/ODBC 服务：

```
./sbin/start-thriftserver.sh
```

该脚本接受所有 ```bin/spark-submit``` 的参数，另外还可以通过 ```--hiveconf``` 选项来指定 Hive 属性。该服务默认监听 ```localhost:10000```，可以通过设置环境变量值来修改：

```
export HIVE_SERVER2_THRIFT_PORT=<listening-port>
export HIVE_SERVER2_THRIFT_BIND_HOST=<listening-host>
./sbin/start-thriftserver.sh \
  --master <master-uri> \
  ...
```

或通过 ```--hiveconf``` 设置：

```
./sbin/start-thriftserver.sh \
  --hiveconf hive.server2.thrift.port=<listening-port> \
  --hiveconf hive.server2.thrift.bind.host=<listening-host> \
  --master <master-uri>
  ...
```

然后使用 beeline 来测试 JDBC/ODBC 服务：

```
./bin/beeline
```

使用 beeline 连接 JDBC/ODBC 服务：

```
beeline> !connect jdbc:hive2://localhost:10000
```

Beeline 需要你提供一个用户名和密码。在非安全模式中，键入机器用户名和空密码即可；在安全模式中，可以按照 [beeline](https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients) 进行设置

Thrift JDBC server 也支持通过 HTTP 传输 RPC 消息，如下设置系统参数或 hive-site.xml 启用 HTTP 模式：

```
hive.server2.transport.mode - Set this to value: http
hive.server2.thrift.http.port - HTTP port number fo listen on; default is 10001
hive.server2.http.endpoint - HTTP endpoint; default is cliservice
```

使用 beeline 来连接 HTTP 模式下的 JDBC/ODBC thrift server：

```
beeline> !connect jdbc:hive2://<host>:<port>/<database>?hive.server2.transport.mode=http;hive.server2.thrift.http.path=<http_endpoint>
```

## 运行 Spark SQL CLI
Spark SQL CLI 是一个很方便的工具，用来以 local 模式执行 Hive metastore 服务和执行查询。注意，Spark SQL CLI 无法和 JDBC thrift server，执行下面命令启动 Spark SQL CLI：

```
./bin/spark-sql
```

## 与 Hive 的兼容性
Spark SQL 被设计成与 Hive Metastore、SerDes 和 UDFs 兼容，并且可以与 Hive 各个版本写作（从0.12.0到1.2.1）。

Spark SQL thrift server 可以与现有已安装的 Hive 兼容，不需要修改当前的 Hive Metastore 或表数据的存放位置。

支持及不支持的 Hive 特性以及具体的数据类型请移步：
https://spark.apache.org/docs/latest/sql-programming-guide.html#compatibility-with-apache-hive

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
